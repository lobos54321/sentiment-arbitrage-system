/**
 * 清理 $NUCLEAR 僵尸仓位
 *
 * 问题：$NUCLEAR 持仓 29+ 小时，entryPrice=0 导致 PnL 异常，
 * 每 1.5 秒产生无效价格查询（4.75h 内 1140 次）
 *
 * 用法：node scripts/cleanup-nuclear.js
 */

import Database from 'better-sqlite3';
import { Connection, PublicKey, Keypair, LAMPORTS_PER_SOL } from '@solana/web3.js';
import bs58 from 'bs58';
import dotenv from 'dotenv';

dotenv.config();

const dbPath = process.env.DB_PATH || './data/sentiment_arb.db';

async function main() {
  console.log('🧹 清理 $NUCLEAR 僵尸仓位\n');

  // 1. 数据库清理
  const db = new Database(dbPath);

  // 检查 live_positions 表是否存在
  const tableExists = db.prepare(
    "SELECT name FROM sqlite_master WHERE type='table' AND name='live_positions'"
  ).get();

  if (tableExists) {
    // 查找 NUCLEAR 仓位
    const nuclearPositions = db.prepare(
      "SELECT * FROM live_positions WHERE symbol LIKE '%NUCLEAR%' OR (status='open' AND entry_price = 0)"
    ).all();

    if (nuclearPositions.length > 0) {
      console.log(`📋 找到 ${nuclearPositions.length} 个需要清理的仓位:`);
      for (const pos of nuclearPositions) {
        console.log(`   - $${pos.symbol} | CA: ${pos.token_ca?.substring(0, 8)}... | entry_price: ${pos.entry_price} | status: ${pos.status}`);
      }

      // 关闭这些仓位
      const result = db.prepare(
        "UPDATE live_positions SET status='closed', exit_reason='ZOMBIE_CLEANUP', exit_pnl=-100, closed_at=? WHERE symbol LIKE '%NUCLEAR%' OR (status='open' AND entry_price = 0)"
      ).run(Date.now());

      console.log(`\n✅ 已关闭 ${result.changes} 个僵尸仓位`);
    } else {
      console.log('ℹ️  live_positions 表中没有找到 NUCLEAR 或 entryPrice=0 的仓位');
    }
  } else {
    console.log('ℹ️  live_positions 表尚未创建（运行时创建）');
  }

  // 2. 检查链上是否还有 NUCLEAR token
  const privateKey = process.env.TRADE_WALLET_PRIVATE_KEY;
  if (privateKey) {
    try {
      const rpcUrl = process.env.SOLANA_RPC_URL || 'https://api.mainnet-beta.solana.com';
      const connection = new Connection(rpcUrl, 'confirmed');
      const wallet = Keypair.fromSecretKey(bs58.decode(privateKey));

      console.log(`\n🔍 检查钱包 ${wallet.publicKey.toBase58().substring(0, 8)}... 中的 token 余额...`);

      // 获取所有 token 账户
      const tokenAccounts = await connection.getParsedTokenAccountsByOwner(
        wallet.publicKey,
        { programId: new PublicKey('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA') }
      );

      const nonZeroTokens = tokenAccounts.value.filter(ta => {
        const amount = parseInt(ta.account.data.parsed.info.tokenAmount.amount);
        return amount > 0;
      });

      if (nonZeroTokens.length > 0) {
        console.log(`\n📦 钱包中有 ${nonZeroTokens.length} 个非零 token:`);
        for (const ta of nonZeroTokens) {
          const info = ta.account.data.parsed.info;
          const mint = info.mint;
          const amount = info.tokenAmount.uiAmount;
          const decimals = info.tokenAmount.decimals;
          console.log(`   ${mint.substring(0, 8)}... | ${amount} tokens (decimals: ${decimals})`);
        }
        console.log('\n⚠️  如果其中有 NUCLEAR 的 token，需要手动 emergencySell 或关闭 token 账户');
      } else {
        console.log('✅ 钱包中没有非零 token 余额');
      }

      // SOL 余额
      const solBalance = await connection.getBalance(wallet.publicKey);
      console.log(`\n💰 SOL 余额: ${(solBalance / LAMPORTS_PER_SOL).toFixed(6)} SOL`);
    } catch (e) {
      console.error(`⚠️  链上检查失败: ${e.message}`);
    }
  } else {
    console.log('\n⚠️  TRADE_WALLET_PRIVATE_KEY 未设置，跳过链上检查');
  }

  db.close();
  console.log('\n🧹 清理完成');
}

main().catch(console.error);
