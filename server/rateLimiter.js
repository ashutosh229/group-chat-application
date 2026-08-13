/**
 * rateLimiter.js
 * ------------------------------------------------------------
 * Per-connection token-bucket rate limiter.
 *
 * Why: the baseline tutorial has no defense against a client
 * (buggy or malicious) hammering `send()` in a loop and flooding
 * every other participant. A token bucket allows short bursts
 * (normal typing/sending behavior) while capping sustained rate.
 *
 * capacity          -> max tokens (= max burst size)
 * refillPerSec       -> tokens regenerated per second
 * ------------------------------------------------------------
 */

class TokenBucket {
  constructor(capacity, refillPerSec) {
    this.capacity = capacity;
    this.refillPerSec = refillPerSec;
    this.tokens = capacity;
    this.lastRefill = Date.now();
  }

  _refill() {
    const now = Date.now();
    const elapsedSec = (now - this.lastRefill) / 1000;
    if (elapsedSec > 0) {
      this.tokens = Math.min(this.capacity, this.tokens + elapsedSec * this.refillPerSec);
      this.lastRefill = now;
    }
  }

  /** Attempt to spend `cost` tokens. Returns true if allowed. */
  tryConsume(cost = 1) {
    this._refill();
    if (this.tokens >= cost) {
      this.tokens -= cost;
      return true;
    }
    return false;
  }
}

module.exports = { TokenBucket };
