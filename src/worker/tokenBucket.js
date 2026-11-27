// Token bucket: caps how many deals per second ONE job may move.
// Valid in-process (no Redis) because the DB guarantees at most one running job per
// workspace — exactly one process spends a workspace's budget at a time.
export class TokenBucket {
  /** @param ratePerSec tokens added per second; @param burst max tokens held */
  constructor(ratePerSec, burst) {
    this.rate = ratePerSec;
    this.burst = burst;
    this.tokens = burst;
    this.last = Date.now();
  }

  /** How long (ms) to wait before n tokens are available; reserves them. */
  reserve(n) {
    const now = Date.now();
    this.tokens = Math.min(this.burst, this.tokens + ((now - this.last) / 1000) * this.rate);
    this.last = now;
    this.tokens -= n;                       // may go negative = debt we wait off
    return this.tokens >= 0 ? 0 : (-this.tokens / this.rate) * 1000;
  }
}
