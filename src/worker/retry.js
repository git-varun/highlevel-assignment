// In-process retry for TRANSIENT failures of one chunk.
// Safe because a chunk is one transaction: a failed attempt rolled back completely.

// Postgres SQLSTATEs + Node socket errors worth retrying.
const TRANSIENT = new Set([
  '40001', // serialization_failure
  '40P01', // deadlock_detected
  '55P03', // lock_not_available (our lock_timeout)
  '57014', // query_canceled (our statement_timeout)
  '57P01', // admin_shutdown (DB restarting)
  '57P02', // crash_shutdown
  '57P03', // cannot_connect_now (DB starting up)
  '25P03', // idle_in_transaction_session_timeout — OUR guard killed a stalled transaction
  '08000', '08003', '08006', // connection exceptions
  'ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'EPIPE',
]);

export const isTransient = (err) =>
  TRANSIENT.has(err?.code) || /Connection terminated|timeout exceeded when trying to connect/i.test(err?.message ?? '');

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Exponential backoff with jitter: ~200ms, 400ms, 800ms… capped. Jitter spreads out
 *  workers that failed at the same moment so they don't retry in lockstep. */
export async function withRetry(fn, { retries = 5, baseMs = 200, capMs = 10_000, onRetry } = {}) {
  for (let attempt = 0; ; attempt++) {
    try {
      return await fn();
    } catch (err) {
      if (!isTransient(err) || attempt >= retries) throw err;
      const exp = Math.min(capMs, baseMs * 2 ** attempt);
      const wait = exp / 2 + Math.random() * (exp / 2);
      await onRetry?.(err, attempt + 1, wait); // awaited: it may renew the lease
      await sleep(wait);
    }
  }
}
