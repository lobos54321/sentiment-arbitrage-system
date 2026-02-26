#!/usr/bin/env node
/**
 * 分析被过滤token的后续表现
 * 验证：七维分过滤是否漏掉了金狗
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const logPath = path.join(__dirname, '..', 'logs', 'restart_v75_20260113_002033.log');

// 读取日志
const log = fs.readFileSync(logPath, 'utf-8');
const lines = log.split('\n');

// 提取被过滤的token
const filteredTokens = new Map();

for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    
    // 匹配 "不入池" 的行
    if (line.includes('不入池')) {
        const match = line.match(/\[Filter\]\s+(.+?)\s+七维分\s+(\d+)/);
        if (match) {
            const symbol = match[1];
            const score = parseInt(match[2]);
            
            // 往上找地址
            let address = null;
            for (let j = i - 1; j >= Math.max(0, i - 50); j--) {
                const prevLine = lines[j];
                
                // 在 market/metrics 中找地址
                if (prevLine.includes('market/metrics')) {
                    const addrMatch = prevLine.match(/"address":"([A-Za-z0-9]+)"/);
                    if (addrMatch) {
                        address = addrMatch[1];
                        break;
                    }
                }
                
                // 在 CrossValidator 验证完成中找
                if (prevLine.includes('[CrossValidator] 验证完成:') && prevLine.includes(symbol)) {
                    // 继续往上找地址
                    for (let k = j - 1; k >= Math.max(0, j - 30); k--) {
                        const searchLine = lines[k];
                        if (searchLine.includes('market/metrics')) {
                            const addrMatch = searchLine.match(/"address":"([A-Za-z0-9]+)"/);
                            if (addrMatch) {
                                address = addrMatch[1];
                                break;
                            }
                        }
                    }
                    break;
                }
            }
            
            if (address && !filteredTokens.has(address)) {
                filteredTokens.set(address, { symbol, score, address });
            }
        }
    }
}

console.log(`\n${'═'.repeat(60)}`);
console.log(`📊 被过滤Token分析`);
console.log(`${'═'.repeat(60)}`);
console.log(`共找到 ${filteredTokens.size} 个唯一的被过滤token`);

// 输出前20个
console.log(`\n前20个被过滤token:`);
let count = 0;
for (const [addr, info] of filteredTokens) {
    if (count >= 20) break;
    console.log(`  ${info.symbol} (分数:${info.score}) -> ${addr.slice(0, 12)}...`);
    count++;
}

// 保存到文件供后续查询
const output = {
    analyzed_at: new Date().toISOString(),
    total_filtered: filteredTokens.size,
    tokens: Array.from(filteredTokens.values())
};

const outputPath = path.join(__dirname, '..', 'data', 'filtered-tokens-to-check.json');
fs.writeFileSync(outputPath, JSON.stringify(output, null, 2));
console.log(`\n已保存到 ${outputPath}`);
