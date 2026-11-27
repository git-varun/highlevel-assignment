// Benchmark: one command, reproducible inputs, writes BENCHMARKS.md.
//   npm run bench        (reseeds the full 500k dataset first — takes ~2-3 minutes)
//
// Processes: API (port 3100) and worker run as SEPARATE processes from this load
// generator, so none of them shares an event loop with the thing measuring it.
// Load is OPEN-LOOP: requests fire on a fixed schedule regardless of how fast earlier
// ones return, so a slow server cannot quietly reduce the load it is measured under.
import { spawn, execSync } from 'node:child_process';
import { once } from 'node:events';
import { writeFile, mkdir } from 'node:fs/promises';
import os from 'node:os';
import { createPool } from '../src/db.js';
import { seed, BIG_WS, SMALL_WS } from './seed.js';
import { verifyJob } from './verify-job.js';

const PORT = 3100;
const BASE = `http://localhost:${PORT}`;
const RATE = Number(process.env.BENCH_RPS ?? 40);        // requests/sec per workspace class
const BASELINE_SECONDS = Number(process.env.BENCH_BASELINE_S ?? 15);
const LEASE = '10';                                        // shorter lease: bounds kill→resume wait
const pool = createPool('bench');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const q1 = async (sql, p) => (await pool.query(sql, p)).rows[0];

// ─── processes ──────────────────────────────────────────────────────────────
function start(script, env = {}) {
  const p = spawn(process.execPath, [script], { env: { ...process.env, PORT: String(PORT), LEASE_SECONDS: LEASE, ...env }, stdio: ['ignore', 'pipe', 'pipe'] });
  p.out = ''; p.stdout.on('data', (d) => { p.out += d; }); p.stderr.on('data', (d) => { p.out += d; });
  return p;
}
async function stop(p, sig = 'SIGTERM') { if (p.exitCode === null) { p.kill(sig); await once(p, 'exit'); } }
async function waitApi() { for (let i = 0; i < 100; i++) { try { if ((await fetch(`${BASE}/health`)).ok) return; } catch {} await sleep(100); } throw new Error('api did not start'); }

// ─── load generator ─────────────────────────────────────────────────────────
/** Interactive traffic for one workspace: 80% list-a-stage reads, 20% single-deal moves. */
function trafficFor(ws, stageIds, dealIds) {
  return async () => {
    const t0 = performance.now();
    let res;
    if (Math.random() < 0.8) {
      const s = stageIds[Math.floor(Math.random() * stageIds.length)];
      res = await fetch(`${BASE}/opportunities?stageId=${s}&limit=50`, { headers: { 'X-Workspace-Id': ws } });
    } else {
      const d = dealIds[Math.floor(Math.random() * dealIds.length)];
      const s = stageIds[Math.floor(Math.random() * stageIds.length)];
      res = await fetch(`${BASE}/opportunities/${d}/move`, {
        method: 'POST', headers: { 'X-Workspace-Id': ws, 'Content-Type': 'application/json' }, body: JSON.stringify({ toStageId: s }) });
    }
    await res.arrayBuffer();
    return { ms: performance.now() - t0, status: res.status };
  };
}
/** Fire `fn` RATE times/sec until stop() — open loop. */
function load(fn) {
  const samples = []; const inflight = new Set(); let timer;
  const fire = () => { const p = fn().then((s) => samples.push(s), () => samples.push({ ms: NaN, status: 0 })); inflight.add(p); p.finally(() => inflight.delete(p)); };
  timer = setInterval(fire, 1000 / RATE);
  return { stop: async () => { clearInterval(timer); await Promise.all(inflight); return samples; } };
}
const pct = (xs, p) => { const s = xs.filter(Number.isFinite).sort((a, b) => a - b); return s.length ? s[Math.min(s.length - 1, Math.ceil((p / 100) * s.length) - 1)] : NaN; };
function summarise(samples) {
  const ms = samples.map((s) => s.ms);
  const errors = samples.filter((s) => s.status === 0 || s.status >= 500).length;
  return { n: samples.length, p50: pct(ms, 50), p95: pct(ms, 95), p99: pct(ms, 99), max: pct(ms, 100), errors };
}
async function measure(classes, untilFn) {
  const gens = Object.fromEntries(Object.entries(classes).map(([k, fn]) => [k, load(fn)]));
  await untilFn();
  const out = {};
  for (const [k, g] of Object.entries(gens)) out[k] = summarise(await g.stop());
  return out;
}

// ─── job helpers ────────────────────────────────────────────────────────────
/** A filter on one stage matching ~50,000 deals (newest by created_at). 10 deals of headroom
 *  below the cap: interactive moves running at the same time may push a deal into the stage. */
async function fiftyK(stageName) {
  const { id: stageId } = await q1(`SELECT id FROM stages WHERE workspace_id = $1 AND name = $2`, [BIG_WS, stageName]);
  const { created_at } = await q1(
    `SELECT created_at FROM opportunities WHERE workspace_id = $1 AND stage_id = $2
     ORDER BY created_at DESC OFFSET 49989 LIMIT 1`, [BIG_WS, stageId]);
  return { stageId, filter: { stageId, createdFrom: created_at.toISOString() } };
}
async function submit(filter, targetStageId, key) {
  const t0 = performance.now();
  const r = await fetch(`${BASE}/bulk-moves`, { method: 'POST',
    headers: { 'X-Workspace-Id': BIG_WS, 'Content-Type': 'application/json', 'Idempotency-Key': key },
    body: JSON.stringify({ targetStageId, filter }) });
  const body = await r.json();
  if (r.status !== 202) throw new Error(`submit failed ${r.status} ${JSON.stringify(body)}`);
  return { id: body.id, total: body.progress.total, submitMs: performance.now() - t0 };
}
const jobRow = (id) => q1(`SELECT status, total_count, applied_count, skipped_count, attempt, failures,
  extract(epoch FROM finished_at - started_at) AS secs FROM bulk_jobs WHERE id = $1`, [id]);
async function untilDone(id) { for (;;) { const j = await jobRow(id); if (['completed', 'failed'].includes(j.status)) return j; await sleep(100); } }

// ─── hardware ───────────────────────────────────────────────────────────────
function hardware() {
  const sh = (c) => { try { return execSync(c, { encoding: 'utf8' }).trim(); } catch { return 'n/a'; } };
  return {
    cpu: os.cpus()[0]?.model ?? 'n/a', cores: os.cpus().length,
    ramGiB: (os.totalmem() / 2 ** 30).toFixed(1), os: `${os.type()} ${os.release()}`,
    node: process.version, postgres: 'n/a', cpuMHz: os.cpus()[0]?.speed, loadAvg: os.loadavg().map((x) => x.toFixed(2)).join(' '),
    container: sh('cat /sys/fs/cgroup/cpu.max 2>/dev/null'),
    note: 'API, worker, load generator and Postgres all on this one machine (they compete for CPU).',
  };
}

// ─── main ───────────────────────────────────────────────────────────────────
async function main() {
  const results = { when: new Date().toISOString(), hardware: hardware(), config: { rps: RATE, lease: Number(LEASE), chunk: 500 } };
  results.hardware.postgres = (await q1('SHOW server_version')).server_version;
  console.log('seeding 500k + 5×3k…'); await seed(pool);

  const stagesOf = async (ws) => (await pool.query('SELECT id FROM stages WHERE workspace_id = $1 ORDER BY position', [ws])).rows.map((r) => r.id);
  const dealsOf = async (ws) => (await pool.query('SELECT id FROM opportunities WHERE workspace_id = $1 ORDER BY random() LIMIT 2000', [ws])).rows.map((r) => r.id);
  const bigTraffic = trafficFor(BIG_WS, await stagesOf(BIG_WS), await dealsOf(BIG_WS));
  const smallTraffic = trafficFor(SMALL_WS[0], await stagesOf(SMALL_WS[0]), await dealsOf(SMALL_WS[0]));
  const classes = { sameWorkspace: bigTraffic, otherWorkspace: smallTraffic };

  const api = start('src/api/server.js'); await waitApi();

  console.log(`baseline: ${BASELINE_SECONDS}s of interactive load, no bulk job…`);
  results.baseline = await measure(classes, () => sleep(BASELINE_SECONDS * 1000));

  const target = (await q1(`SELECT id FROM stages WHERE workspace_id = $1 AND name = 'On Hold'`, [BIG_WS])).id;
  for (const [label, rate, stage] of [['paced', '5000', 'Contacted'], ['unpaced', '0', 'Discovery']]) {
    console.log(`bulk move 50k (${label}) under interactive load…`);
    const { filter } = await fiftyK(stage);
    const worker = start('src/worker/index.js', { BULK_RATE_PER_SEC: rate });
    let job;
    const lat = await measure(classes, async () => {
      job = await submit(filter, target, `bench-${label}-${Date.now()}`);
      job.row = await untilDone(job.id);
    });
    await stop(worker);
    const v = await verifyJob(pool, job.id);
    results[label] = { ratePerSec: Number(rate) || 'unlimited', total: job.total, submitMs: job.submitMs,
      seconds: Number(job.row.secs), throughput: job.total / Number(job.row.secs), verified: v.ok, latency: lat };
  }

  console.log('kill -9 at 30,000 of 50,000, then resume…');
  {
    const { filter } = await fiftyK('New Lead');
    let w = start('src/worker/index.js');
    const job = await submit(filter, target, `bench-kill-${Date.now()}`);
    while ((await jobRow(job.id)).applied_count < 30_000) await sleep(20);
    w.kill('SIGKILL'); await once(w, 'exit');
    const tKill = performance.now();
    const atKill = await jobRow(job.id);
    w = start('src/worker/index.js');
    const done = await untilDone(job.id);
    const resumeSecs = (performance.now() - tKill) / 1000;
    await stop(w);
    const v = await verifyJob(pool, job.id);
    results.killResume = { total: job.total, appliedAtKill: atKill.applied_count, secondsKillToDone: resumeSecs,
      attempt: done.attempt, failures: done.failures, verified: v.ok, checks: v.checks,
      resumedFrom: (w.out.match(/resume_from=(\d+)/) ?? [])[1] };
  }

  await stop(api);
  await mkdir('bench', { recursive: true });
  const tag = process.env.BENCH_TAG;                 // e.g. BENCH_TAG=stress BENCH_RPS=150
  if (tag) {
    await writeFile(`bench/results-${tag}.json`, JSON.stringify(results, null, 2));
    console.log(`wrote bench/results-${tag}.json`);
  } else {
    await writeFile('bench/results.json', JSON.stringify(results, null, 2));
    await writeFile('BENCHMARKS.md', render(results));
    console.log('wrote bench/results.json and BENCHMARKS.md');
  }
}

const f = (n, d = 1) => (Number.isFinite(n) ? n.toFixed(d) : 'n/a');
function latRow(name, l) { return `| ${name} | ${l.n} | ${f(l.p50)} | ${f(l.p95)} | ${f(l.p99)} | ${f(l.max)} | ${l.errors} |`; }
import { existsSync, readFileSync } from 'node:fs';
function stressSection() {
  if (!existsSync('bench/results-stress.json')) return '';
  const s = JSON.parse(readFileSync('bench/results-stress.json', 'utf8'));
  const pctChange = (a, b) => `${b >= a ? '+' : ''}${(((b - a) / a) * 100).toFixed(0)}%`;
  const row = (name, l, base) => `| ${name} | ${f(l.p95)} (${pctChange(base.p95, l.p95)}) | ${f(l.p99)} (${pctChange(base.p99, l.p99)}) | ${l.errors} |`;
  return `
### 2b. Stress run: ${s.config.rps} req/s per workspace class (\`BENCH_TAG=stress BENCH_RPS=${s.config.rps} npm run bench\`)

At the light load above the machine is not saturated, so pacing has nothing to protect.
This run raises interactive load until the bulk job's cost becomes visible.

| Scenario | p95 ms (vs baseline) | p99 ms (vs baseline) | errors |
|---|---|---|---|
| Baseline — same workspace | ${f(s.baseline.sameWorkspace.p95)} | ${f(s.baseline.sameWorkspace.p99)} | ${s.baseline.sameWorkspace.errors} |
${row('Paced — same workspace', s.paced.latency.sameWorkspace, s.baseline.sameWorkspace)}
${row('Unpaced — same workspace', s.unpaced.latency.sameWorkspace, s.baseline.sameWorkspace)}
| Baseline — other workspace | ${f(s.baseline.otherWorkspace.p95)} | ${f(s.baseline.otherWorkspace.p99)} | ${s.baseline.otherWorkspace.errors} |
${row('Paced — other workspace', s.paced.latency.otherWorkspace, s.baseline.otherWorkspace)}
${row('Unpaced — other workspace', s.unpaced.latency.otherWorkspace, s.baseline.otherWorkspace)}

Bulk wall-clock in this run: paced ${f(s.paced.seconds)} s (${f(s.paced.throughput, 0)}/s), unpaced ${f(s.unpaced.seconds)} s (${f(s.unpaced.throughput, 0)}/s); all verified ${s.paced.verified && s.unpaced.verified ? '✅' : '❌'}.

**Reading it honestly:**
- Pacing is a real, measured trade-off: unpaced finishes ~${f(s.paced.seconds / s.unpaced.seconds)}× faster but raises
  same-workspace p95 by ${pctChange(s.baseline.sameWorkspace.p95, s.unpaced.latency.sameWorkspace.p95)}; paced keeps the increase to ${pctChange(s.baseline.sameWorkspace.p95, s.paced.latency.sameWorkspace.p95)}.
- The OTHER workspace moves about as much as the same one. Application-level isolation
  (separate pools, one running job per workspace, per-job pacing) holds, but every tenant
  shares one Postgres and — here — 2 CPUs. That shared database is the hole the isolation
  mechanism still leaves; see DESIGN.md.
`;
}

function render(r) {
  const h = r.hardware;
  return `# Benchmarks

Generated by \`npm run bench\` on ${r.when.slice(0, 10)}. Re-run it to reproduce: it reseeds the
same deterministic dataset (\`setseed(0.42)\`: 500,000 deals in the large workspace, 5 × 3,000).

## Hardware

| | |
|---|---|
| CPU | ${h.cpu} (${h.cores} cores) |
| RAM | ${h.ramGiB} GiB |
| OS | ${h.os} |
| Node / Postgres | ${h.node} / ${h.postgres} |

${h.note} Absolute numbers are therefore pessimistic; compare each run against the baseline.

## 1. Bulk move of 50,000

| Run | Pacing | Deals | Submit (snapshot) | Wall-clock | Sustained throughput | Verified |
|---|---|---|---|---|---|---|
| paced | ${r.paced.ratePerSec}/s per job | ${r.paced.total} | ${f(r.paced.submitMs, 0)} ms | ${f(r.paced.seconds)} s | ${f(r.paced.throughput, 0)} deals/s | ${r.paced.verified ? '✅ all invariants' : '❌'} |
| unpaced | ${r.unpaced.ratePerSec} | ${r.unpaced.total} | ${f(r.unpaced.submitMs, 0)} ms | ${f(r.unpaced.seconds)} s | ${f(r.unpaced.throughput, 0)} deals/s | ${r.unpaced.verified ? '✅ all invariants' : '❌'} |

Wall-clock = \`finished_at − started_at\` from the database (excludes the submit request).
Chunk size ${r.config.chunk}, one running job per workspace, worker pool of 4 connections.

## 2. Interactive latency during the bulk run (ms)

Open-loop load: ${r.config.rps} req/s per workspace class (80% list-a-stage reads, 20% single-deal
moves on random deals — some of them inside the job's set), fired on a fixed schedule.

| Scenario | Requests | p50 | p95 | p99 | max | 5xx/errors |
|---|---|---|---|---|---|---|
${latRow('Baseline — same workspace (no bulk job)', r.baseline.sameWorkspace)}
${latRow('Baseline — other workspace (no bulk job)', r.baseline.otherWorkspace)}
${latRow('Paced bulk — same workspace', r.paced.latency.sameWorkspace)}
${latRow('Paced bulk — other workspace', r.paced.latency.otherWorkspace)}
${latRow('Unpaced bulk — same workspace', r.unpaced.latency.sameWorkspace)}
${latRow('Unpaced bulk — other workspace', r.unpaced.latency.otherWorkspace)}
${stressSection()}
## 3. Kill and resume

| | |
|---|---|
| Job size | ${r.killResume.total} |
| Killed (\`SIGKILL\`) at | ${r.killResume.appliedAtKill} applied |
| Kill → completed | ${f(r.killResume.secondsKillToDone)} s (includes waiting out the ${r.config.lease} s lease) |
| Resumed from cursor | opportunity id ${r.killResume.resumedFrom} |
| attempt / failures | ${r.killResume.attempt} / ${r.killResume.failures} |
| Correct | ${r.killResume.verified ? '✅' : '❌'} |

Proof of correctness (\`scripts/verify-job.js\`, computed from raw tables, not the status column):

${Object.entries(r.killResume.checks).map(([k, v]) => `- ${v ? '✅' : '❌'} ${k}`).join('\n')}

## Method notes and caveats

- Load generator, API, worker and Postgres share one machine; the generator's own CPU use
  inflates latencies. Relative change vs baseline is the meaningful signal.
- Kill→resume time is dominated by the lease (${r.config.lease} s here; 30 s by default). That is the
  price of crash detection without a coordinator; a shorter lease detects faster but risks
  reclaiming a job from a worker that is merely slow.
- Single runs, not averages: expect ± variation between runs.
`;
}

// `node scripts/bench.js --render` rebuilds BENCHMARKS.md from saved results (no re-run).
if (process.argv.includes('--render')) {
  const { readFile } = await import('node:fs/promises');
  const r = JSON.parse(await readFile('bench/results.json', 'utf8'));
  await writeFile('BENCHMARKS.md', render(r));
  console.log('rendered BENCHMARKS.md');
  await pool.end();
} else {
  main().catch((e) => { console.error(e); process.exitCode = 1; }).finally(() => pool.end());
}
