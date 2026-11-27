// Worker process: N concurrent slots, each looping claim → chunks → complete.
import os from 'node:os';
import { createPool } from '../db.js';
import { TokenBucket } from './tokenBucket.js';
import { withRetry, sleep, isTransient } from './retry.js';
import {
  claimJob, snapshotJob, runChunk, heartbeat, completeJob, releaseJob, reapExhausted,
  LostOwnershipError, LEASE_SECONDS,
} from './jobs.js';

const cfg = {
  concurrency: Number(process.env.WORKER_CONCURRENCY ?? 2),     // jobs in parallel (different workspaces)
  chunkSize: Number(process.env.CHUNK_SIZE ?? 500),
  ratePerSec: Number(process.env.BULK_RATE_PER_SEC ?? 5_000),   // per job; 0 = unlimited
  pollMs: Number(process.env.POLL_MS ?? 500),
  reapEveryMs: 10_000,
};

export function startWorker(pool, { id = `${os.hostname()}:${process.pid}`, log = console.log } = {}) {
  let stopping = false;

  async function processJob(job) {
    const bucket = cfg.ratePerSec > 0 ? new TokenBucket(cfg.ratePerSec, cfg.chunkSize) : null;
    let lastRenew = Date.now();
    log(`[worker ${id}] claimed job ${job.id} attempt=${job.attempt} failures=${job.failures} resume_from=${job.cursor_id}`);

    try {
      if (!job.snapshot_at) {
        const s = await withRetry(() => snapshotJob(pool, job));
        if (s.failed) { log(`[worker ${id}] job ${job.id} failed at start: too many matches`); return; }
        log(`[worker ${id}] job ${job.id} snapshot: ${s.total} deals`);
        lastRenew = Date.now();
      }
      while (true) {
        if (stopping) {
          await releaseJob(pool, job, { graceful: true });
          log(`[worker ${id}] released job ${job.id} for shutdown`);
          return;
        }
        const r = await withRetry(() => runChunk(pool, job, cfg.chunkSize), {
          onRetry: (e, n, ms) => log(`[worker ${id}] chunk retry #${n} in ${ms | 0}ms: ${e.code ?? e.message}`),
        });
        lastRenew = Date.now(); // the chunk commit renewed the lease
        if (r.done) {
          const ok = await completeJob(pool, job);
          log(`[worker ${id}] job ${job.id} ${ok ? 'completed' : 'FAILED completion check'}`);
          return;
        }
        // Pacing: wait off the token debt; keep the lease alive if the wait is long.
        let wait = bucket ? bucket.reserve(r.processed) : 0;
        while (wait > 0 && !stopping) {
          const step = Math.min(wait, 1000);
          await sleep(step);
          wait -= step;
          if (Date.now() - lastRenew > (LEASE_SECONDS * 1000) / 3) {
            await heartbeat(pool, job);
            lastRenew = Date.now();
          }
        }
      }
    } catch (err) {
      if (err instanceof LostOwnershipError) {
        // Someone else owns it now (we were a zombie). Our open chunk already rolled back.
        log(`[worker ${id}] ${err.message} — stopping work on it`);
        return;
      }
      log(`[worker ${id}] job ${job.id} error: ${err.message}`);
      // Transient errors that exhausted in-process retries, or a real bug: count a failure,
      // back off, let any worker retry later. After max_attempts → 'failed', never stuck.
      await releaseJob(pool, job, { graceful: false, error: String(err.message).slice(0, 500) })
        .catch((e) => log(`[worker ${id}] could not release job ${job.id}: ${e.message} (lease will expire)`));
    }
  }

  async function slot(n) {
    while (!stopping) {
      try {
        const job = await claimJob(pool, `${id}#${n}`);
        if (job) await processJob(job);
        else await sleep(cfg.pollMs);
      } catch (err) {
        // DB down etc. Never crash the loop; back off and try again.
        log(`[worker ${id}#${n}] loop error: ${err.message}`);
        await sleep(isTransient(err) ? 2000 : 5000);
      }
    }
  }

  async function reaper() {
    while (!stopping) {
      try {
        const failed = await reapExhausted(pool);
        if (failed.length) log(`[worker ${id}] reaper failed exhausted jobs: ${failed.join(', ')}`);
      } catch (err) { log(`[worker ${id}] reaper error: ${err.message}`); }
      for (let t = 0; t < cfg.reapEveryMs && !stopping; t += 250) await sleep(250); // interruptible
    }
  }

  const running = Promise.all([...Array.from({ length: cfg.concurrency }, (_, n) => slot(n)), reaper()]);
  return {
    stop: async () => { stopping = true; await running; },
  };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const pool = createPool('worker');
  const w = startWorker(pool);
  console.log(`[worker] started`, cfg);
  for (const sig of ['SIGTERM', 'SIGINT']) {
    process.on(sig, async () => {
      console.log(`[worker] ${sig}: finishing current chunk, handing jobs back`);
      setTimeout(() => process.exit(1), 9_000).unref();  // under docker's 10s SIGKILL
      await w.stop();
      await pool.end();
      process.exit(0);
    });
  }
}
