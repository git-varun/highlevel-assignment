#!/bin/sh
# Proves the tests guard real mechanisms: break ONE safety mechanism at a time, run the
# test meant to catch it, and require that test to FAIL. The code is restored after each.
#   sh scripts/prove-tests.sh
set -u
cd "$(dirname "$0")/.."
if ! git diff --quiet -- src; then echo "src/ has uncommitted changes; commit or stash first"; exit 2; fi

# A failing test only means something if the same test PASSES on the real code.
# Without this baseline, a broken suite would report every mutation as "caught".
echo "baseline: full suite on unmodified code…"
if ! npm test >/dev/null 2>&1; then echo "baseline FAILED — fix the suite first; mutation results would be meaningless"; exit 2; fi
echo "baseline green\n"

caught=0; missed=0
mutate() { # $1 label, $2 file, $3 sed expression, $4 test file, $5 test name pattern
  sed -i "$3" "$2"
  if git diff --quiet -- "$2"; then echo "?? $1: mutation did not apply"; missed=$((missed+1)); return; fi
  if node --test --test-timeout=30000 --test-name-pattern="$5" "$4" >/dev/null 2>&1; then
    echo "✘ MISSED  $1  (test still passed with the mechanism removed)"; missed=$((missed+1))
  else
    echo "✔ caught  $1"; caught=$((caught+1))
  fi
  git checkout -q -- "$2"
}

mutate "version check (manual edit wins)" src/worker/jobs.js \
  '/AND o.version = u.expected_version/d' test/worker.test.js 'changed after the snapshot'

mutate "fencing on attempt (zombie workers)" src/worker/jobs.js \
  's/attempt = \$2/$2::int = $2::int/g' test/worker.test.js 'zombie'

mutate "claim livelock fix (expired running job)" src/worker/jobs.js \
  "s/WHERE r.workspace_id = j.workspace_id AND r.status = 'running'))/WHERE r.workspace_id = j.workspace_id AND r.status = 'running' AND r.lease_until >= now()))/" \
  test/worker.test.js 'livelock'

mutate "request-hash check (same key, different body)" src/domain/bulkMoves.js \
  's/prior.rows\[0\].request_hash !== req.requestHash/false/' test/idempotency.test.js 'different body'

mutate "per-workspace submit lock (cap under concurrency)" src/domain/bulkMoves.js \
  '/pg_advisory_xact_lock/d' test/idempotency.test.js 'serialised'

mutate "state-before-version (single move retry is not 409)" src/domain/opportunities.js \
  '/if (opp.stage_id === toStageId) return/d' test/idempotency.test.js 'single move'

mutate "completion invariant (never fake 100%)" src/worker/jobs.js \
  '/AND j.applied_count + j.skipped_count = j.total_count/d' test/worker.test.js 'counters do not add up'

mutate "floor percent (49,999/50,000 is not 100%)" src/domain/bulkMoves.js \
  's/Math.floor((done/Math.round((done/' test/worker.test.js 'never 100%'

mutate "unknown filter keys rejected (typo ≠ everything)" src/domain/bulkMoves.js \
  's/if (unknown.length) throw/if (false) throw/' test/api.test.js 'hostile'

mutate "snapshot excludes deals already in target" src/domain/bulkMoves.js \
  "s/'stage_id <> \$2'/'\$2::bigint IS NOT NULL'/" test/api.test.js 'filter matrix'

mutate "tenant scoping on single move (404 across workspaces)" src/domain/opportunities.js \
  's/FROM opportunities WHERE id = \$1 AND workspace_id = \$2 FOR UPDATE/FROM opportunities WHERE id = $1 AND $2::uuid IS NOT NULL FOR UPDATE/' \
  test/api.test.js 'tenancy'

echo "\n$caught caught, $missed missed"

# Mutated code can write data the real code never would (e.g. a cross-tenant row).
# Prove the suite is still green afterwards, i.e. nothing leaked into later runs.
echo "after: full suite on restored code…"
if npm test >/dev/null 2>&1; then echo "after green"; else echo "after FAILED — a mutation leaked state"; exit 1; fi
[ "$missed" -eq 0 ]
