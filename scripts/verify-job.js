// Prove a bulk job's outcome from first principles — don't trust its status column.
//   node scripts/verify-job.js <jobId>
import { createPool } from '../src/db.js';

export async function verifyJob(pool, jobId) {
  const q = async (sql) => (await pool.query(sql, [jobId])).rows[0];
  const job = await q('SELECT * FROM bulk_jobs WHERE id = $1');
  const items = await q(`SELECT count(*)::int total,
      count(*) FILTER (WHERE status = 'pending')::int pending,
      count(*) FILTER (WHERE status = 'applied')::int applied,
      count(*) FILTER (WHERE status = 'skipped_modified')::int skipped
    FROM bulk_job_items WHERE job_id = $1`);
  const tr = await q(`SELECT count(*)::int n, count(DISTINCT opportunity_id)::int distinct_opps
    FROM stage_transitions WHERE job_id = $1`);
  // Applied deals must sit in the target stage, unless something moved them AFTER this job
  // did: a user, or a bulk job submitted after this one finished (its submit-time snapshot
  // then legitimately contains deals this job moved).
  const misplaced = await q(`SELECT count(*)::int n FROM bulk_job_items i
    JOIN bulk_jobs j ON j.id = i.job_id JOIN opportunities o ON o.id = i.opportunity_id
    WHERE i.job_id = $1 AND i.status = 'applied' AND o.stage_id <> j.target_stage_id
      AND NOT EXISTS (SELECT 1 FROM stage_transitions t WHERE t.opportunity_id = o.id
                      AND t.id > (SELECT max(id) FROM stage_transitions
                      WHERE job_id = $1 AND opportunity_id = o.id))`);
  // Skipped deals must really have been modified (version moved past the snapshot).
  const wrongSkips = await q(`SELECT count(*)::int n FROM bulk_job_items i
    JOIN opportunities o ON o.id = i.opportunity_id
    WHERE i.job_id = $1 AND i.status = 'skipped_modified' AND o.version = i.expected_version`);

  const checks = {
    'job status is completed': job.status === 'completed',
    'no pending items': items.pending === 0,
    'applied + skipped = total': items.applied + items.skipped === items.total && items.total === job.total_count,
    'job counters match item rows': job.applied_count === items.applied && job.skipped_count === items.skipped,
    'one transition per applied item': tr.n === items.applied,
    'no duplicate transitions': tr.n === tr.distinct_opps,
    'applied deals are in target stage (or moved again later)': misplaced.n === 0,
    'every skip was a real concurrent modification': wrongSkips.n === 0,
  };
  return { job: { status: job.status, total: job.total_count, applied: job.applied_count,
    skipped: job.skipped_count, attempt: job.attempt, failures: job.failures,
    seconds: job.finished_at && (job.finished_at - job.started_at) / 1000 }, checks,
    ok: Object.values(checks).every(Boolean) };
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const pool = createPool('verify');
  verifyJob(pool, process.argv[2])
    .then((r) => {
      console.log(r.job);
      for (const [k, v] of Object.entries(r.checks)) console.log(`${v ? '✔' : '✘'} ${k}`);
      console.log(r.ok ? '\nALL CHECKS PASSED' : '\nCHECKS FAILED');
      process.exitCode = r.ok ? 0 : 1;
    })
    .finally(() => pool.end());
}
