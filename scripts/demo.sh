#!/usr/bin/env bash
# Guided demo for the video walkthrough. Press Enter to advance each step.
#
#   bash scripts/demo.sh            # full demo (needs the full seed: npm run seed)
#   bash scripts/demo.sh --prep     # run once BEFORE recording: saves the mutation-proof output
#   NO_PAUSE=1 bash scripts/demo.sh # run straight through (rehearsal)
#
# Needs: node, curl, a migrated + seeded database at $DATABASE_URL. Port 3000 must be free.
set -uo pipefail
cd "$(dirname "$0")/.."

PORT=${PORT:-3000}; export PORT
export LEASE_SECONDS=${LEASE_SECONDS:-10}   # short lease so the crash demo doesn't wait 30s
export BULK_RATE_PER_SEC=${BULK_RATE_PER_SEC:-5000}
API="http://localhost:$PORT"
BIG=00000000-0000-0000-0000-000000000001
SMALL2=00000000-0000-0000-0000-000000000002
SMALL3=00000000-0000-0000-0000-000000000003
LOGS=$(mktemp -d)

B=$'\e[1m'; C=$'\e[36m'; G=$'\e[32m'; Y=$'\e[33m'; R=$'\e[31m'; D=$'\e[2m'; N=$'\e[0m'
step()  { printf "\n${B}${C}━━ %s ━━${N}\n" "$1"; }
say()   { printf "${D}%s${N}\n" "$1"; }
show()  { printf "${Y}\$ %s${N}\n" "$1"; }          # print a command before running it
pause() { [ "${NO_PAUSE:-0}" = 1 ] || read -rp $'\n'"${D}⏎ Enter for next step…${N} " _; }
sql()   { node scripts/demo-sql.js "$@"; }
json()  { node -e "const j=JSON.parse(require('fs').readFileSync(0,'utf8'));console.log($1)"; }

if [ "${1:-}" = "--prep" ]; then
  echo "Running the mutation proof (takes a few minutes)…"
  mkdir -p bench
  sh scripts/prove-tests.sh | tee bench/prove-tests-output.txt
  echo "Saved to bench/prove-tests-output.txt — the demo will display it."
  exit 0
fi

API_PID=; WORKER_PID=
start_api()    { node src/api/server.js > "$LOGS/api.log" 2>&1 & API_PID=$!; for _ in $(seq 50); do curl -sf "$API/health" >/dev/null && return; sleep 0.1; done; echo "API did not start (port $PORT busy?)"; exit 1; }
start_worker() { node src/worker/index.js >> "$LOGS/worker.log" 2>&1 & WORKER_PID=$!; }
stop_worker()  { [ -n "$WORKER_PID" ] && kill -TERM "$WORKER_PID" 2>/dev/null && wait "$WORKER_PID" 2>/dev/null; WORKER_PID=; }
cleanup()      { stop_worker; [ -n "$API_PID" ] && kill "$API_PID" 2>/dev/null; rm -rf "$LOGS"; }
trap cleanup EXIT

submit() {  # submit <workspace> <key> <json-body>  → prints status line, sets JOB
  local out; out=$(curl -s -D "$LOGS/h" -H "X-Workspace-Id: $1" -H 'Content-Type: application/json' \
    -H "Idempotency-Key: $2" -X POST "$API/bulk-moves" -d "$3")
  local code; code=$(head -1 "$LOGS/h" | awk '{print $2}')
  local replay; replay=$(grep -i '^idempotent-replayed' "$LOGS/h" | tr -d '\r' | awk '{print $2}')
  JOB=$(echo "$out" | json "j.id ?? ''")
  if [ "$code" = 202 ]; then
    printf "  ${G}%s${N}  job=%s  total=%s  replayed=%s\n" "$code" "${JOB:0:8}…" "$(echo "$out" | json 'j.progress.total')" "${replay:-false}"
  else
    printf "  ${R}%s${N}  %s\n" "$code" "$(echo "$out" | json 'j.error.code+": "+j.error.message')"
  fi
}
progress() { curl -s -H "X-Workspace-Id: $1" "$API/bulk-moves/$2" | json \
  '`status=${j.status}  health=${j.health}  applied=${j.progress.applied}/${j.progress.total}  ${j.progress.percent}%  skipped=${j.progress.skipped}`'; }
watch_until() {  # watch_until <ws> <job> <shell-test on $P>  — live single-line progress
  while :; do P=$(progress "$1" "$2"); printf "\r  %-90s" "$P"; eval "$3" && break; sleep 0.4; done; echo
}

# ── preflight ───────────────────────────────────────────────────────────────
npm run -s migrate >/dev/null || { echo "Database unreachable or migration failed"; exit 1; }
DEALS=$(sql "SELECT count(*) FROM opportunities WHERE workspace_id = \$1" "$BIG")
[ "${DEALS:-0}" -ge 100000 ] || { echo "Large workspace has $DEALS deals. Run: npm run seed"; exit 1; }
sql "UPDATE bulk_jobs SET status = 'failed', error = 'demo reset' WHERE status IN ('queued','running')" >/dev/null
start_api

clear
step "Bulk Stage Move — live demo"
say "Postgres is both the data and the queue: job state, cursor and deal updates commit in one transaction."
say "API (pid $API_PID) on :$PORT · $DEALS deals in the large workspace · lease ${LEASE_SECONDS}s"
pause

# ── 1. idempotency ──────────────────────────────────────────────────────────
step "1 · Idempotency — a retry gets the same job, never a 409"
S2=$(sql "SELECT id FROM stages WHERE workspace_id = \$1 ORDER BY position" "$SMALL2" | head -2 | paste -sd' ')
FROM2=${S2% *}; TO2=${S2#* }
KEY="demo-$(date +%s)"
BODY="{\"targetStageId\":$TO2,\"filter\":{\"stageId\":$FROM2}}"
show "POST /bulk-moves   Idempotency-Key: $KEY"
submit "$SMALL2" "$KEY" "$BODY"
show "same request again (client retry)"
submit "$SMALL2" "$KEY" "$BODY"
show "same key, DIFFERENT body"
submit "$SMALL2" "$KEY" "{\"targetStageId\":$TO2,\"filter\":{\"stageId\":$FROM2,\"status\":\"won\"}}"
show "10 identical requests in parallel, new key"
KEY10="demo10-$(date +%s)"
# xargs -P runs them in parallel. (Not "&" + a bare "wait": that would also wait for the API
# server this script started in the background, and hang forever.)
seq 10 | xargs -P 10 -I{} curl -s -o /dev/null -w "%{http_code}\n" -H "X-Workspace-Id: $SMALL2" -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $KEY10" -X POST "$API/bulk-moves" -d "{\"targetStageId\":$FROM2,\"filter\":{\"stageId\":$TO2}}" | sort | uniq -c | sed 's/^/  /'
printf "  jobs created for that key: ${G}%s${N}\n" "$(sql "SELECT count(*) FROM bulk_jobs WHERE idempotency_key = \$1" "$KEY10")"
sql "UPDATE bulk_jobs SET status = 'failed', error = 'demo cleanup' WHERE workspace_id = \$1 AND status = 'queued'" "$SMALL2" >/dev/null
pause

# ── 2. crash + resume ───────────────────────────────────────────────────────
step "2 · Crash and resume — kill -9 at ~60% of a ~50,000-deal job"
# Use whatever the data looks like NOW (earlier runs/benchmarks move deals around):
# the fullest stage, capped just under 50,000 via a created_at cut-off.
IFS='|' read -r STAGE STAGE_NAME STAGE_N <<< "$(sql "SELECT s.id, s.name, count(*) FROM opportunities o JOIN stages s ON s.id = o.stage_id
  WHERE o.workspace_id = \$1 GROUP BY s.id, s.name ORDER BY count(*) DESC LIMIT 1" "$BIG")"
IFS='|' read -r TARGET TARGET_NAME <<< "$(sql "SELECT id, name FROM stages WHERE workspace_id = \$1 AND id <> \$2 ORDER BY position DESC LIMIT 1" "$BIG" "$STAGE")"
FILTER="{\"stageId\":$STAGE}"
if [ "$STAGE_N" -gt 49990 ]; then
  FROM_TS=$(sql "SELECT created_at FROM opportunities WHERE workspace_id = \$1 AND stage_id = \$2 ORDER BY created_at DESC OFFSET 49989 LIMIT 1" "$BIG" "$STAGE")
  FILTER="{\"stageId\":$STAGE,\"createdFrom\":\"$FROM_TS\"}"
fi
show "POST /bulk-moves   $STAGE_NAME → $TARGET_NAME"
submit "$BIG" "crash-$(date +%s)" "{\"targetStageId\":$TARGET,\"filter\":$FILTER}"
[ -n "$JOB" ] || { echo "submit failed — stopping"; exit 1; }
CRASH_JOB=$JOB
TOTAL=$(sql "SELECT total_count FROM bulk_jobs WHERE id = \$1" "$CRASH_JOB")
KILL_AT=$(( TOTAL * 6 / 10 ))
show "npm run worker"
start_worker
watch_until "$BIG" "$CRASH_JOB" "[[ \$P =~ applied=([0-9]+) ]] && [ \"\${BASH_REMATCH[1]}\" -ge $KILL_AT ]"
show "kill -9 $WORKER_PID      # no cleanup possible"
kill -9 "$WORKER_PID"; wait "$WORKER_PID" 2>/dev/null; WORKER_PID=
say "The job still says running… until its lease expires. Then health turns 'stalled'."
watch_until "$BIG" "$CRASH_JOB" '[[ $P == *stalled* ]]'
pause
show "npm run worker      # a fresh worker"
start_worker
sleep 1.5
printf "  ${G}%s${N}\n" "$(grep claimed "$LOGS/worker.log" | tail -1 | sed 's/.*claimed/claimed/')"
say "resume_from = the committed cursor: it continues where the last chunk committed, not from zero."
watch_until "$BIG" "$CRASH_JOB" '[[ $P == *completed* || $P == *failed* ]]'
show "node scripts/verify-job.js $CRASH_JOB"
node scripts/verify-job.js "$CRASH_JOB" | sed 's/^/  /'
stop_worker
pause

# ── 3. manual edit wins ─────────────────────────────────────────────────────
step "3 · A user edits a deal after the job was submitted — the user wins"
# fullest stage as the source; two other stages as the job's target and the user's choice
FROM3=$(sql "SELECT stage_id FROM opportunities WHERE workspace_id = \$1 GROUP BY 1 ORDER BY count(*) DESC LIMIT 1" "$SMALL3")
read -r TO3 LOST3 <<< "$(sql "SELECT id FROM stages WHERE workspace_id = \$1 AND id <> \$2 ORDER BY position LIMIT 2" "$SMALL3" "$FROM3" | paste -sd' ')"
show "POST /bulk-moves   (worker is stopped, so the job waits)"
submit "$SMALL3" "manual-$(date +%s)" "{\"targetStageId\":$TO3,\"filter\":{\"stageId\":$FROM3}}"
[ -n "$JOB" ] || { echo "submit failed — stopping"; exit 1; }
EDIT_JOB=$JOB
DEAL=$(sql "SELECT opportunity_id FROM bulk_job_items WHERE job_id = \$1 ORDER BY 1 DESC LIMIT 1" "$EDIT_JOB")
show "POST /opportunities/$DEAL/move   → stage $LOST3   (a person moves one of the job's deals)"
curl -s -H "X-Workspace-Id: $SMALL3" -H 'Content-Type: application/json' -X POST "$API/opportunities/$DEAL/move" \
  -d "{\"toStageId\":$LOST3}" | json '"  200  stage_id="+j.stage_id+"  version="+j.version'
show "npm run worker"
start_worker
watch_until "$SMALL3" "$EDIT_JOB" '[[ $P == *completed* || $P == *failed* ]]'
printf "  deal %s: stage=${G}%s${N} (the user's choice)   history: %s\n" "$DEAL" \
  "$(sql "SELECT stage_id FROM opportunities WHERE id = \$1" "$DEAL")" \
  "$(sql "SELECT string_agg(from_stage_id || '→' || to_stage_id || ' ' || source, ', ' ORDER BY id) FROM stage_transitions WHERE opportunity_id = \$1" "$DEAL")"
say "The job skipped it (version changed since the snapshot) instead of overwriting it."
stop_worker
pause

# ── 4. tests ────────────────────────────────────────────────────────────────
step "4 · Tests against real Postgres"
show "npm test"
npm test 2>&1 | grep -E "^ok|^not ok|^# (tests|pass|fail)" | sed 's/^/  /'
pause
step "5 · Mutation proof — each safety mechanism removed, its test must fail"
if [ -f bench/prove-tests-output.txt ]; then
  show "sh scripts/prove-tests.sh   (recorded earlier: bash scripts/demo.sh --prep)"
  sed 's/^/  /' bench/prove-tests-output.txt
else
  say "Run 'bash scripts/demo.sh --prep' before recording to show this step."
fi
pause

step "Done"
say "DESIGN.md: every trade-off, a reverted design, a withdrawn benchmark claim, and the known weak spot."
