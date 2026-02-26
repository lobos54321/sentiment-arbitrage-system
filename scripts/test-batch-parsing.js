
import fs from 'fs';
import path from 'path';

// Mock the BatchAIAdvisor's processAIResponse logic (v1.3 with strict commands)
function simulateParsing(content, tokens) {
    const buyTokens = [];
    const watchTokens = [];
    const discardTokens = [];

    // v2: 基于章节解析（更准确）
    // 尝试匹配建议章节的标题（从后往前找，确保匹配最后一个“最终建议”等章节）
    const opSectionHeaderRegex = /^(?:#+\s*|\d+\.\s*|第[一二三四五六七八九十]+\s*[步章节][：:]\s*)?[#* ]*(?:操作建议|最终建议|最终决策|结论|决策建议|Final\s*Decision|Final\s*Recommendation)[#*：: ]*/img;
    let lastHeaderMatch = null;
    let match;
    while ((match = opSectionHeaderRegex.exec(content)) !== null) {
        lastHeaderMatch = match;
    }

    if (lastHeaderMatch) {
        const opSection = content.substring(lastHeaderMatch.index);
        const opLines = opSection.split('\n');

        for (const line of opLines) {
            const upperLine = line.toUpperCase();

            for (const token of tokens) {
                const symbol = token.symbol;
                const address = token.address;

                // 1. 优先通过地址匹配 (CA)
                const addressRegex = new RegExp(address.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
                // 2. 备选通过符号匹配
                const symbolRegex = new RegExp(`\\$?${symbol.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}(?![a-zA-Z0-9])`, 'i');

                if (addressRegex.test(line) || symbolRegex.test(line)) {
                    const lineClean = line.replace(/[*#]/g, '').trim();
                    // 构造更严格的动作指令正则：动作词 必须紧邻标点或位于行核心位置
                    const isActionLine = (
                        new RegExp(`(?:BUY|买入|入手|WATCH_ENTRY|再买|到位买|WATCH|观察|再看|DISCARD|丢弃|放弃|排除)[\\s:：]`, 'i').test(lineClean) ||
                        new RegExp(`[:：\\s-](?:BUY|买入|入手|WATCH_ENTRY|再买|到位买|WATCH|观察|再看|DISCARD|丢弃|放弃|排除)`, 'i').test(lineClean)
                    );

                    if (!isActionLine) continue;

                    if (upperLine.includes('BUY') || upperLine.includes('买入') || upperLine.includes('入手')) {
                        if (!buyTokens.includes(token)) {
                            const tierMatch = line.match(/TIER[_\s]?(S|A|B|C)/i);
                            if (tierMatch) {
                                token.intentionTier = `TIER_${tierMatch[1].toUpperCase()}`;
                            }
                            buyTokens.push(token);
                        }
                    }
                    else if (upperLine.includes('WATCH_ENTRY') || upperLine.includes('再买') || upperLine.includes('到位买')) {
                        if (!buyTokens.includes(token) && !watchTokens.includes(token)) {
                            const priceMatch = line.match(/目标价格[：:\s]*\$?([\d.]+)/i) ||
                                line.match(/\$?([\d.]+)\s*再买/i) ||
                                line.match(/\$([\d.]+)/);
                            if (priceMatch) {
                                token.targetEntryPrice = parseFloat(priceMatch[1]);
                                token.isWatchEntry = true;
                            }
                            const tierMatch = line.match(/TIER[_\s]?(S|A|B|C)/i);
                            if (tierMatch) {
                                token.intentionTier = `TIER_${tierMatch[1].toUpperCase()}`;
                            }
                            watchTokens.push(token);
                        }
                    }
                    else if (upperLine.includes('WATCH') || upperLine.includes('观察') || upperLine.includes('再看')) {
                        if (!buyTokens.includes(token) && !watchTokens.includes(token)) {
                            watchTokens.push(token);
                        }
                    }
                    else if (upperLine.includes('DISCARD') || upperLine.includes('丢弃') || upperLine.includes('放弃') || upperLine.includes('排除')) {
                        if (!buyTokens.includes(token) && !watchTokens.includes(token) && !discardTokens.includes(token)) {
                            discardTokens.push(token);
                        }
                    }
                }
            }
        }
    } else {
        tokens.forEach(token => {
            const symbolUpper = token.symbol.toUpperCase();
            const address = token.address.toUpperCase();

            const caEscaped = address.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
            const symEscaped = symbolUpper.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
            const contextRegex = new RegExp(`(?:.{0,30})(?:${caEscaped}|\\$?${symEscaped})(?:.{0,30})`, 'i');
            const contextMatch = content.match(contextRegex);

            if (contextMatch) {
                const contextText = contextMatch[0].toUpperCase();
                if (contextText.includes('BUY') || contextText.includes('买入') || contextText.includes('入手')) {
                    buyTokens.push(token);
                } else if (contextText.includes('DISCARD') || contextText.includes('丢弃') || contextText.includes('排除')) {
                    discardTokens.push(token);
                } else if (contextText.includes('WATCH') || contextText.includes('观察') || contextText.includes('再看')) {
                    watchTokens.push(token);
                }
            }
        });
    }
    return { buyTokens, watchTokens, discardTokens };
}

// Test tokens
const testTokens = [
    { symbol: 'PinkFloyd', address: 'addr1' },
    { symbol: 'LYM', address: 'addr2' },
    { symbol: 'Alone', address: 'addr3' },
    { symbol: '继续积累吧', address: 'addr4' },
    { symbol: '0xabd7af...', address: '0xabd7af73e52a36666b1d358517c5302bb4784444' }
];

const testContents = [
    {
        name: "Standard Action Recommendation",
        content: `
**操作建议**
- BUY: $PinkFloyd (CA: addr1) (叙事等级 TIER_B)
- DISCARD: $LYM (CA: addr2)
`
    },
    {
        name: "Final Recommendation with Reverse Order",
        content: `
**最终建议**
- $PinkFloyd (CA: addr1): BUY, TIER_B
- $LYM (CA: addr2): DISCARD
- $Alone (CA: addr3): WATCH_ENTRY, 目标价格 $0.5
`
    },
    {
        name: "Real Log 22:14 (BSC Address Symbol)",
        content: `
### 第七步：最终建议
**BUY: $0xabd7af... (CA: 0xabd7af73e52a36666b1d358517c5302bb4784444) (叙事等级 TIER_A, 目标市值 $1M)** — 现在买，有40x空间 (匹配卖出策略：SM高/Liq稳，警惕1min-20%/热度<30%)。
`
    }
];

testContents.forEach(tc => {
    console.log(`\nTesting: ${tc.name}`);
    const tokens = testTokens.map(t => ({ ...t }));
    const result = simulateParsing(tc.content, tokens);

    console.log("  BUY:", result.buyTokens.map(t => `${t.symbol}${t.intentionTier ? ` (${t.intentionTier})` : ''}`));
    console.log("  WATCH:", result.watchTokens.map(t => `${t.symbol}${t.targetEntryPrice ? ` @$${t.targetEntryPrice}` : ''}`));
    console.log("  DISCARD:", result.discardTokens.map(t => t.symbol));
});
