/**
 * 获取 Telegram Chat ID 的辅助脚本
 *
 * 使用方法：
 * 1. 先向你的 Bot 发送一条消息（在 Telegram 中）
 * 2. 运行：node scripts/get-chat-id.js
 */

import TelegramBot from 'node-telegram-bot-api';
import dotenv from 'dotenv';

dotenv.config();

const token = process.env.TELEGRAM_BOT_TOKEN;

if (!token) {
  console.error('❌ 错误：未找到 TELEGRAM_BOT_TOKEN');
  console.error('请在 .env 文件中配置 TELEGRAM_BOT_TOKEN');
  process.exit(1);
}

console.log('🔍 正在获取最新消息...\n');

const bot = new TelegramBot(token, { polling: false });

// 获取最新的更新
bot.getUpdates({ limit: 10 })
  .then(updates => {
    if (updates.length === 0) {
      console.log('❌ 没有找到任何消息！');
      console.log('');
      console.log('请先完成以下步骤：');
      console.log('1. 在 Telegram 中搜索你的 Bot');
      console.log('2. 点击 "Start" 或发送任意消息（如 "Hello"）');
      console.log('3. 再次运行此脚本');
      process.exit(0);
    }

    console.log('✅ 找到消息！\n');

    // 从最新的消息中提取 Chat ID
    const latestUpdate = updates[updates.length - 1];
    const message = latestUpdate.message || latestUpdate.edited_message;

    if (message && message.chat) {
      const chatId = message.chat.id;
      const chatType = message.chat.type;
      const firstName = message.chat.first_name || '';
      const username = message.chat.username || '';

      console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
      console.log('📋 你的 Chat ID 信息：');
      console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
      console.log(`Chat ID: ${chatId}`);
      console.log(`类型: ${chatType}`);
      console.log(`名字: ${firstName}`);
      console.log(`用户名: @${username}`);
      console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
      console.log('');
      console.log('💾 请将以下内容添加到 .env 文件：');
      console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
      console.log(`TELEGRAM_ADMIN_CHAT_ID=${chatId}`);
      console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
      console.log('');

      // 发送测试消息验证
      console.log('📤 正在发送测试消息到你的 Telegram...');
      bot.sendMessage(chatId, '✅ 连接成功！你的 Chat ID 已获取。')
        .then(() => {
          console.log('✅ 测试消息发送成功！');
          console.log('请检查你的 Telegram，应该会收到一条消息。');
          process.exit(0);
        })
        .catch(err => {
          console.error('❌ 发送测试消息失败:', err.message);
          process.exit(1);
        });

    } else {
      console.log('❌ 无法从消息中提取 Chat ID');
      console.log('更新内容:', JSON.stringify(latestUpdate, null, 2));
      process.exit(1);
    }

  })
  .catch(error => {
    console.error('❌ 错误:', error.message);

    if (error.response && error.response.statusCode === 401) {
      console.error('');
      console.error('Bot Token 无效！');
      console.error('请检查 .env 文件中的 TELEGRAM_BOT_TOKEN 是否正确。');
      console.error('');
      console.error('获取正确的 Token：');
      console.error('1. 在 Telegram 中与 @BotFather 对话');
      console.error('2. 发送 /mybots');
      console.error('3. 选择你的 Bot');
      console.error('4. 点击 "API Token"');
    }

    process.exit(1);
  });
