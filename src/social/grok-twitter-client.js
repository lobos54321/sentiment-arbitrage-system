/**
 * Grok API Twitter Client
 *
 * Uses xAI Grok API to search Twitter/X for token mentions
 * Provides Twitter social data for Soft Score calculations
 */

import https from 'https';

class GrokTwitterClient {
  constructor(apiKey) {
    this.apiKey = apiKey || process.env.XAI_API_KEY;
    this.baseURL = 'https://api.x.ai/v1';
    this.model = 'grok-4-1-fast';

    if (!this.apiKey) {
      console.warn('⚠️  XAI_API_KEY not found - Grok Twitter client will not work');
    }
  }

  /**
   * Search Twitter for token mentions
   *
   * @param {string} tokenSymbol - Token symbol (e.g., 'BONK')
   * @param {string} tokenCA - Token contract address
   * @param {number} timeframeMinutes - Search timeframe in minutes (default: 15)
   * @returns {Promise<Object>} Twitter data
   */
  async searchToken(tokenSymbol, tokenCA, timeframeMinutes = 15) {
    if (!this.apiKey) {
      throw new Error('XAI_API_KEY not configured');
    }

    const query = `$${tokenSymbol}`;

    const prompt = `
I need you to search Twitter/X for recent tweets about: ${query}

Look for tweets from the last ${timeframeMinutes} minutes.

Analyze what you find and provide this information in JSON format:
{
  "mention_count": <number of tweets mentioning ${query}>,
  "unique_authors": <number of different accounts posting>,
  "engagement": <total likes + retweets across all tweets>,
  "sentiment": "<positive/neutral/negative based on tweet content>",
  "kol_count": <number of influencers with >10k followers>,
  "top_tweets": [
    {
      "text": "tweet content",
      "author": "@username",
      "engagement": <likes + retweets>
    }
  ]
}

Return ONLY the JSON, no other text.
`;

    try {
      const result = await this._callGrokAPI(prompt);

      // Parse JSON from response
      let data;
      try {
        let content = result.choices[0].message.content;

        // 🛠️ Enhanced JSON extraction logic - handles various response formats
        // Try method 1: Extract from ```json code block
        const jsonBlockMatch = content.match(/```json\n([\s\S]*?)\n```/);
        if (jsonBlockMatch) {
          content = jsonBlockMatch[1];
        } else {
          // Try method 2: Extract from ``` code block
          const codeBlockMatch = content.match(/```\n([\s\S]*?)\n```/);
          if (codeBlockMatch) {
            content = codeBlockMatch[1];
          } else {
            // Try method 3: Extract first { to last } (find JSON object)
            const jsonMatch = content.match(/\{[\s\S]*\}/);
            if (jsonMatch) {
              content = jsonMatch[0];
            }
          }
        }

        // Try to parse
        data = JSON.parse(content);

      } catch (parseError) {
        console.warn(`⚠️  Grok response parsing failed. Using default values.`);
        console.warn(`   Error: ${parseError.message}`);
        console.warn(`   Content sample: ${result.choices[0].message.content.substring(0, 100)}...`);

        // 🛡️ Fallback: return safe empty object to prevent system crash
        data = {
          mention_count: 0,
          unique_authors: 0,
          engagement: 0,
          sentiment: 'neutral',
          kol_count: 0,
          top_tweets: []
        };
      }

      // Add metadata
      data.source = 'grok_api';
      data.token_symbol = tokenSymbol;
      data.token_ca = tokenCA;
      data.timeframe_minutes = timeframeMinutes;
      data.timestamp = new Date().toISOString();

      // Token usage for cost tracking
      data.tokens_used = result.usage ? result.usage.total_tokens : 0;

      console.log(`✅ Grok Twitter search: ${query} - ${data.mention_count} mentions, ${data.engagement} engagement`);

      return data;

    } catch (error) {
      console.error(`❌ Grok Twitter search failed for ${query}:`, error.message);
      throw error;
    }
  }

  /**
   * Call Grok API
   *
   * @private
   * @param {string} userPrompt - User prompt
   * @returns {Promise<Object>} API response
   */
  _callGrokAPI(userPrompt) {
    return new Promise((resolve, reject) => {
      const requestData = JSON.stringify({
        model: this.model,
        messages: [
          {
            role: 'system',
            content: 'You are a Twitter data analyst with access to Twitter/X data. Search for recent tweets and analyze them. Always return valid JSON only, no other text.'
          },
          {
            role: 'user',
            content: userPrompt
          }
        ],
        temperature: 0.3,
        max_tokens: 2000
      });

      const options = {
        hostname: 'api.x.ai',
        port: 443,
        path: '/v1/chat/completions',
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(requestData),
          'Authorization': `Bearer ${this.apiKey}`
        }
      };

      const req = https.request(options, (res) => {
        let data = '';

        res.on('data', (chunk) => {
          data += chunk;
        });

        res.on('end', () => {
          if (res.statusCode !== 200) {
            reject(new Error(`Grok API error: ${res.statusCode} - ${data}`));
            return;
          }

          try {
            const result = JSON.parse(data);
            resolve(result);
          } catch (parseError) {
            reject(new Error(`Failed to parse Grok API response: ${parseError.message}`));
          }
        });
      });

      req.on('error', (error) => {
        reject(new Error(`Grok API request failed: ${error.message}`));
      });

      req.write(requestData);
      req.end();
    });
  }

  /**
   * Validate Telegram signal against Twitter activity
   *
   * @param {string} tokenSymbol - Token symbol
   * @param {string} tokenCA - Token contract address
   * @param {Date} tgMentionTime - Time of Telegram mention
   * @returns {Promise<Object>} Validation result with credibility score
   */
  async validateSignal(tokenSymbol, tokenCA, tgMentionTime) {
    try {
      // Search Twitter for the token
      const twitterData = await this.searchToken(tokenSymbol, tokenCA, 15);

      // Calculate credibility score (0-100)
      let credibilityScore = 0;
      const reasons = [];

      // Twitter activity (max 40 points)
      if (twitterData.mention_count >= 20) {
        credibilityScore += 40;
        reasons.push(`High Twitter activity (${twitterData.mention_count} mentions)`);
      } else if (twitterData.mention_count >= 10) {
        credibilityScore += 25;
        reasons.push(`Moderate Twitter activity (${twitterData.mention_count} mentions)`);
      } else if (twitterData.mention_count >= 5) {
        credibilityScore += 15;
        reasons.push(`Some Twitter activity (${twitterData.mention_count} mentions)`);
      }

      // KOL mentions (max 30 points)
      if (twitterData.kol_count >= 3) {
        credibilityScore += 30;
        reasons.push(`Multiple KOL mentions (${twitterData.kol_count} KOLs)`);
      } else if (twitterData.kol_count >= 1) {
        credibilityScore += 20;
        reasons.push(`KOL mentioned (${twitterData.kol_count} KOL)`);
      }

      // Engagement (max 20 points)
      if (twitterData.engagement >= 1000) {
        credibilityScore += 20;
        reasons.push(`High engagement (${twitterData.engagement})`);
      } else if (twitterData.engagement >= 500) {
        credibilityScore += 15;
        reasons.push(`Good engagement (${twitterData.engagement})`);
      }

      // Sentiment (max 10 points)
      if (twitterData.sentiment === 'positive') {
        credibilityScore += 10;
        reasons.push('Positive sentiment');
      } else if (twitterData.sentiment === 'neutral') {
        credibilityScore += 5;
        reasons.push('Neutral sentiment');
      }

      const verified = credibilityScore >= 50;

      return {
        credibility_score: Math.min(credibilityScore, 100),
        verified,
        reasons,
        twitter_data: twitterData
      };

    } catch (error) {
      console.error('Grok signal validation failed:', error.message);
      return {
        credibility_score: 0,
        verified: false,
        reasons: [`Grok API error: ${error.message}`],
        twitter_data: null
      };
    }
  }
}

export default GrokTwitterClient;
