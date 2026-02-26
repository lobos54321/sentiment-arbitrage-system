/**
 * Telegram User API Authentication Script
 *
 * This script helps you authenticate with Telegram User API
 * and generate the TELEGRAM_SESSION string for GMGN Bot control
 */

import { TelegramClient } from 'telegram';
import { StringSession } from 'telegram/sessions/index.js';
import input from 'input';
import dotenv from 'dotenv';

dotenv.config();

async function authenticate() {
  console.log('\n' + '═'.repeat(80));
  console.log('🔐 TELEGRAM USER API AUTHENTICATION');
  console.log('═'.repeat(80));
  console.log('This will authenticate your Telegram account for GMGN Bot control\n');

  // Check credentials
  const apiId = parseInt(process.env.TELEGRAM_API_ID || '0');
  const apiHash = process.env.TELEGRAM_API_HASH || '';
  const sessionString = process.env.TELEGRAM_SESSION || '';

  if (!apiId || !apiHash) {
    console.error('❌ ERROR: Missing TELEGRAM_API_ID or TELEGRAM_API_HASH');
    console.error('\nPlease add these to your .env file:');
    console.error('1. Visit: https://my.telegram.org/apps');
    console.error('2. Create an application');
    console.error('3. Add to .env:');
    console.error('   TELEGRAM_API_ID=your_api_id');
    console.error('   TELEGRAM_API_HASH=your_api_hash\n');
    process.exit(1);
  }

  console.log('✅ API credentials found');
  console.log(`   API ID: ${apiId}`);
  console.log(`   API Hash: ${apiHash.substring(0, 10)}...\n`);

  if (sessionString) {
    console.log('⚠️  WARNING: You already have a TELEGRAM_SESSION in .env');
    console.log('Continuing will replace it with a new session.\n');

    const confirm = await input.text('Continue? (yes/no): ');
    if (confirm.toLowerCase() !== 'yes') {
      console.log('❌ Cancelled');
      process.exit(0);
    }
  }

  console.log('━'.repeat(80));
  console.log('📱 Starting authentication process...\n');

  const session = new StringSession(sessionString);
  const client = new TelegramClient(session, apiId, apiHash, {
    connectionRetries: 5,
  });

  try {
    await client.start({
      phoneNumber: async () => {
        console.log('\n📞 STEP 1: Enter your phone number');
        console.log('Format: +[country code][phone number]');
        console.log('Example: +8613800138000\n');
        return await input.text('Phone number: ');
      },
      password: async () => {
        console.log('\n🔒 STEP 3: Enter your 2FA password (if enabled)');
        console.log('If you haven\'t enabled 2FA, this step will be skipped\n');
        return await input.text('Password: ');
      },
      phoneCode: async () => {
        console.log('\n💬 STEP 2: Enter verification code');
        console.log('Check your Telegram app for a 5-digit code from "Telegram"\n');
        return await input.text('Code: ');
      },
      onError: (err) => {
        console.error('\n❌ Authentication error:', err.message);
      },
    });

    // Get session string
    const savedSession = client.session.save();

    console.log('\n' + '═'.repeat(80));
    console.log('🎉 AUTHENTICATION SUCCESSFUL!');
    console.log('═'.repeat(80));
    console.log('\n💾 IMPORTANT: Save this session string to your .env file:\n');
    console.log('━'.repeat(80));
    console.log('TELEGRAM_SESSION=' + savedSession);
    console.log('━'.repeat(80));

    console.log('\n📝 Steps to complete setup:');
    console.log('1. Copy the entire session string above');
    console.log('2. Open .env file in a text editor');
    console.log('3. Find the line: TELEGRAM_SESSION=');
    console.log('4. Paste the session string after the =');
    console.log('5. Save the .env file');
    console.log('6. Restart the system with: npm start\n');

    console.log('✅ After saving, you won\'t need to authenticate again!\n');

    await client.disconnect();
    process.exit(0);

  } catch (error) {
    console.error('\n❌ Authentication failed:', error.message);

    if (error.message.includes('PHONE_NUMBER_INVALID')) {
      console.error('\n💡 TIP: Make sure to include the country code (e.g., +86...)');
    } else if (error.message.includes('PHONE_CODE_INVALID')) {
      console.error('\n💡 TIP: The code might have expired. Try again and enter it faster.');
    } else if (error.message.includes('SESSION_PASSWORD_NEEDED')) {
      console.error('\n💡 TIP: You have 2FA enabled. Make sure to enter your password correctly.');
    }

    process.exit(1);
  }
}

// Run authentication
console.log('\n🚀 Starting Telegram authentication...\n');
authenticate().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});
