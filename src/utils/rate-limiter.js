/**
 * Token Bucket Rate Limiter
 *
 * Implements a token bucket algorithm to control API request rate.
 * Prevents exceeding Alchemy's rate limits (25 RPS on free tier).
 *
 * Usage:
 *   const limiter = new RateLimiter(10, 5);  // 10 RPS, burst of 5
 *   await limiter.throttle();  // Wait for permission to make request
 */

export class RateLimiter {
  /**
   * @param {number} requestsPerSecond - Target requests per second
   * @param {number} burstCapacity - Maximum burst size (token bucket capacity)
   */
  constructor(requestsPerSecond, burstCapacity) {
    this.tokens = burstCapacity;      // Current tokens in bucket
    this.maxTokens = burstCapacity;    // Bucket capacity
    this.refillRate = requestsPerSecond; // Tokens added per second
    this.lastRefill = Date.now();
    this.queue = [];                    // Waiting queue (for future use)

    console.log(`⏱️  Rate Limiter initialized: ${requestsPerSecond} RPS, burst ${burstCapacity}`);
  }

  /**
   * Refill tokens based on time passed
   * @private
   */
  refill() {
    const now = Date.now();
    const timePassed = (now - this.lastRefill) / 1000; // seconds
    const newTokens = timePassed * this.refillRate;

    if (newTokens > 0) {
      this.tokens = Math.min(this.maxTokens, this.tokens + newTokens);
      this.lastRefill = now;
    }
  }

  /**
   * Wait for permission to make a request
   * Consumes 1 token. If no tokens available, waits until token is refilled.
   *
   * @param {number} tokenCost - Number of tokens to consume (default: 1)
   * @returns {Promise<void>}
   */
  async throttle(tokenCost = 1) {
    this.refill();

    // If we have enough tokens, consume and return immediately
    if (this.tokens >= tokenCost) {
      this.tokens -= tokenCost;
      return Promise.resolve();
    }

    // Calculate wait time needed to get enough tokens
    const tokensNeeded = tokenCost - this.tokens;
    const waitTime = (tokensNeeded / this.refillRate) * 1000; // milliseconds

    // Wait and retry
    return new Promise(resolve => {
      setTimeout(() => {
        this.throttle(tokenCost).then(resolve);
      }, waitTime);
    });
  }

  /**
   * Get current available tokens
   * @returns {number}
   */
  getAvailableTokens() {
    this.refill();
    return Math.floor(this.tokens);
  }

  /**
   * Reset the rate limiter
   */
  reset() {
    this.tokens = this.maxTokens;
    this.lastRefill = Date.now();
    this.queue = [];
  }
}

export default RateLimiter;
