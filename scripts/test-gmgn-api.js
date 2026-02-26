import { chromium } from 'playwright-extra';
import stealth from 'puppeteer-extra-plugin-stealth';
import fs from 'fs';

chromium.use(stealth());

(async () => {
    const browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({
        userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    });
    
    // Load session
    try {
        const session = JSON.parse(fs.readFileSync('./config/gmgn_session.json'));
        await context.addCookies(session.cookies || []);
        console.log('Session loaded');
    } catch (e) {
        console.log('No session:', e.message);
    }
    
    const page = await context.newPage();
    
    const capturedApis = [];
    
    // Capture ALL API calls
    page.on('response', async (response) => {
        const url = response.url();
        if (url.includes('gmgn.ai') && response.status() === 200) {
            const shortUrl = url.split('?')[0].replace('https://gmgn.ai', '');
            if (shortUrl.includes('/api/') || shortUrl.includes('/defi/') || shortUrl.includes('rank') || shortUrl.includes('trader')) {
                console.log(`API: ${shortUrl}`);
                capturedApis.push(shortUrl);
            }
        }
    });
    
    console.log('=== Loading smart_traders page ===');
    await page.goto('https://gmgn.ai/sol/smart_traders?tab=smart_degen', { 
        waitUntil: 'networkidle',
        timeout: 60000 
    });
    
    // Wait for data to load
    await page.waitForTimeout(10000);
    
    // Take screenshot before scrolling
    await page.screenshot({ path: './logs/gmgn-before-scroll.png', fullPage: false });
    console.log('Screenshot saved');
    
    // Get page title and URL to verify we're on the right page
    console.log('Page URL:', page.url());
    console.log('Page title:', await page.title());
    
    // List unique APIs
    console.log('\n=== Unique APIs captured ===');
    [...new Set(capturedApis)].forEach(api => console.log(api));
    
    await browser.close();
})();
