
import { chromium } from 'playwright-extra';
import stealth from 'puppeteer-extra-plugin-stealth';
import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs';

chromium.use(stealth());

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SESSION_PATH = path.join(__dirname, '../config/debot_session.json');

async function checkStatus() {
    console.log('🔍 Checking DeBot Login Status...');

    if (!fs.existsSync(SESSION_PATH)) {
        console.error('❌ Session file not found!');
        return;
    }

    const browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({
        storageState: SESSION_PATH,
        userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    });
    const page = await context.newPage();

    console.log('🌐 Navigating to DeBot...');
    await page.goto('https://debot.ai/?chain=solana', { waitUntil: 'networkidle' });
    await page.waitForTimeout(5000);

    const screenshotPath = path.join(__dirname, '../debot_status.png');
    try {
        await page.screenshot({ path: screenshotPath });
        console.log(`📸 Screenshot saved to: ${screenshotPath}`);
    } catch (e) {
        console.log(`⚠️ Screenshot failed: ${e.message}`);
    }

    // Check for login button or user profile
    const content = await page.content();
    if (content.includes('Login') || content.includes('Sign in')) {
        console.log('❌ NOT LOGGED IN: "Login" or "Sign in" text found.');
    } else {
        console.log('✅ SEEMS LOGGED IN: "Login" text NOT found.');
    }

    await browser.close();
}

checkStatus().catch(console.error);
