/**
 * 测试 Telegram Bot 配置
 *
 * 验证：
 * 1. Bot Token 是否有效
 * 2. Admin Chat ID 是否正确
 * 3. 能否成功发送消息
 */

import TelegramBot from 'node-telegram-bot-api';
import dotenv from 'dotenv';

dotenv.config();

const token = process.env.TELEGRAM_BOT_TOKEN;
const chatId = process.env.TELEGRAM_ADMIN_CHAT_ID;

console.log('🧪 测试 Telegram Bot 配置\n');

// 检查配置
if (!token) {
  console.error('❌ 未找到 TELEGRAM_BOT_TOKEN');
  process.exit(1);
}

if (!chatId) {
  console.error('❌ 未找到 TELEGRAM_ADMIN_CHAT_ID');
  process.exit(1);
}

console.log('✅ 配置文件检查通过');
console.log(`   Bot Token: ${token.substring(0, 20)}...`);
console.log(`   Chat ID: ${chatId}\n`);

const bot = new TelegramBot(token, { polling: false });

// 测试 1: 验证 Bot Token
console.log('📋 测试 1: 验证 Bot Token...');
bot.getMe()
  .then(botInfo => {
    console.log('✅ Bot Token 有效');
    console.log(`   Bot 名称: ${botInfo.first_name}`);
    console.log(`   Bot 用户名: @${botInfo.username}\n`);

    // 测试 2: 发送测试消息
    console.log('📤 测试 2: 发送测试消息...');
    const testMessage = `
🧪 **Telegram Bot 配置测试**

✅ Bot Token: 有效
✅ Chat ID: ${chatId}
✅ 连接: 成功

系统已准备就绪！🎉
    `.trim();

    return bot.sendMessage(chatId, testMessage, { parse_mode: 'Markdown' });
  })
  .then(() => {
    console.log('✅ 测试消息发送成功！');
    console.log('   请检查你的 Telegram，应该收到一条测试消息。\n');

    // 测试 3: 发送带按钮的消息
    console.log('📤 测试 3: 发送交互式消息...');
    const keyboard = {
      inline_keyboard: [
        [
          { text: '✅ 配置完成', callback_data: 'config_done' },
          { text: '📖 查看文档', url: 'https://github.com/anthropics/claude-code' }
        ]
      ]
    };

    return bot.sendMessage(
      chatId,
      '点击下方按钮测试交互功能：',
      { reply_markup: keyboard }
    );
  })
  .then(() => {
    console.log('✅ 交互式消息发送成功！\n');
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('🎉 所有测试通过！');
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('\n✅ Telegram Bot 配置完全正确！');
    console.log('✅ 系统可以正常发送通知了。\n');
    process.exit(0);
  })
  .catch(error => {
    console.error('\n❌ 测试失败:', error.message);

    if (error.response && error.response.statusCode === 401) {
      console.error('\n原因: Bot Token 无效');
      console.error('解决方案: 检查 .env 中的 TELEGRAM_BOT_TOKEN');
    } else if (error.response && error.response.statusCode === 400) {
      console.error('\n原因: Chat ID 无效或 Bot 未启动');
      console.error('解决方案: ');
      console.error('1. 确认已在 Telegram 中向 Bot 发送过消息');
      console.error('2. 检查 .env 中的 TELEGRAM_ADMIN_CHAT_ID');
    }

    console.error('');
    process.exit(1);
  });
