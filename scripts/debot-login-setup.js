/**
 * DeBot 登录快照脚本 - 只需运行一次
 * 
 * 使用方法:
 * node scripts/debot-login-setup.js
 * 
 * 这个脚本会:
 * 1. 打开一个浏览器窗口
 * 2. 你手动登录 DeBot
 * 3. 登录成功后按回车，保存 Session
 */

import { chromium } from 'playwright-extra';
import stealth from 'puppeteer-extra-plugin-stealth';
import path from 'path';
import { fileURLToPath } from 'url';
import readline from 'readline';

// 加载 Stealth 插件（绕过检测）
chromium.use(stealth());

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SESSION_PATH = path.join(__dirname, '../config/debot_session.json');

async function waitForEnter(prompt) {
    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
    });
    
    return new Promise(resolve => {
        rl.question(prompt, () => {
            rl.close();
            resolve();
        });
    });
}

async function loginSetup() {
    console.log('═'.repeat(60));
    console.log('🔐 DeBot 登录快照工具');
    console.log('═'.repeat(60));
    console.log('');
    
    // 启动有头浏览器
    const browser = await chromium.launch({ 
        headless: false,
        args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-blink-features=AutomationControlled'
        ]
    });
    
    const context = await browser.newContext({
        userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        viewport: { width: 1920, height: 1080 }
    });
    
    const page = await context.newPage();
    
    console.log('📱 正在打开 DeBot...');
    await page.goto('https://debot.ai/', { 
        waitUntil: 'load',
        timeout: 120000 
    });
    
    // 额外等待
    await page.waitForTimeout(5000);
    
    console.log('');
    console.log('━'.repeat(60));
    console.log('👉 请在浏览器中手动登录 DeBot');
    console.log('   (可能需要扫码或输入验证码)');
    console.log('');
    console.log('👉 登录成功后，回到这里按 Enter 键保存 Session...');
    console.log('━'.repeat(60));
    
    await waitForEnter('\n按 Enter 继续...');
    
    // 保存 Session
    console.log('\n💾 正在保存登录状态...');
    await context.storageState({ path: SESSION_PATH });
    
    console.log('');
    console.log('═'.repeat(60));
    console.log('✅ 登录态已保存到:', SESSION_PATH);
    console.log('');
    console.log('🚀 现在可以运行系统了，它会自动使用你的登录状态!');
    console.log('═'.repeat(60));
    
    await browser.close();
    process.exit(0);
}

// 运行
loginSetup().catch(err => {
    console.error('❌ 错误:', err.message);
    process.exit(1);
});
