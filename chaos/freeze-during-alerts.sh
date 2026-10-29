#!/usr/bin/env bash
# Chaos: freeze (docker pause = SIGSTOP of the cgroup) the node sending 1,200 pickup alerts for
# FREEZE_SECONDS, let another node take the run over, unpause, and prove the frozen node was fenced.
#
#   1. same setup as kill-during-alerts.sh; read the owner's fencing counters from /actuator/prometheus
#   2. once the sink shows $FREEZE_AFTER deliveries, `docker pause` the owner for $FREEZE_SECONDS
#   3. `docker unpause`, wait for SUCCEEDED and for the frozen node to report the lost lease
#   4. assert: frozen node's scheduler_stale_commit_rejected_total OR scheduler_lease_lost_total rose;
#      sink staleRejected > 0 for the run (warning only, see below); deliveredDistinct == $EMPLOYEES;
#      maxDeliveriesPerKey == 1
#
# Usage: chaos/freeze-during-alerts.sh
# Env:   see chaos/lib.sh and chaos/README.md; FREEZE_SECONDS (30), FREEZE_AFTER (300),
#        FENCE_WAIT (30: seconds to wait after unpause for the frozen node's counters to move).
set -euo pipefail

# shellcheck source=chaos/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

case "${1:-}" in
    "") ;;
    -h | --help)
        sed -n '2,15p' "${BASH_SOURCE[0]}"
        exit 0
        ;;
    *) die "unknown argument '$1'" ;;
esac

FREEZE_SECONDS="${FREEZE_SECONDS:-30}"
FREEZE_AFTER="${FREEZE_AFTER:-300}"
FENCE_WAIT="${FENCE_WAIT:-30}"
PAUSED=""

cleanup() {
    local status=$?
    if [[ -n "${PAUSED}" ]]; then
        docker unpause "${PAUSED}" > /dev/null 2>&1 || warn "could not unpause ${PAUSED}"
    fi
    sink_clear_faults
    disable_job
    exit "${status}"
}
trap cleanup EXIT

require_tools
cd "${REPO_ROOT}"

((FREEZE_SECONDS > LEASE_SECONDS)) \
    || die "FREEZE_SECONDS (${FREEZE_SECONDS}) must exceed the ${LEASE_SECONDS} s run lease, or nothing is taken over"

setup_alert_run "chaos-freeze"
((FREEZE_AFTER < EMPLOYEES)) || die "FREEZE_AFTER (${FREEZE_AFTER}) must be below EMPLOYEES (${EMPLOYEES})"
NODE_URL="$(scheduler_url "${OWNER}")"

# Fencing counters of the node about to be frozen (Prometheus names from EngineMetrics):
#   scheduler_stale_commit_rejected_total            fenced commit matched 0 rows
#   scheduler_lease_lost_total{reason="stale_409"}   sink answered 409 STALE_TOKEN
#   scheduler_lease_lost_total{reason="fenced_write_0_rows"}  renew/checkpoint/commit matched 0 rows
#   scheduler_lease_lost_total{reason="self_fence"}  local monotonic deadline passed before a send
read_counters() {
    STALE_COMMIT="$(prom_sum "${NODE_URL}" scheduler_stale_commit_rejected_total)"
    LL_409="$(prom_sum "${NODE_URL}" scheduler_lease_lost_total 'reason="stale_409"')"
    LL_FENCED="$(prom_sum "${NODE_URL}" scheduler_lease_lost_total 'reason="fenced_write_0_rows"')"
    LL_SELF="$(prom_sum "${NODE_URL}" scheduler_lease_lost_total 'reason="self_fence"')"
}
read_counters
B_STALE_COMMIT="${STALE_COMMIT}"
B_LL_409="${LL_409}"
B_LL_FENCED="${LL_FENCED}"
B_LL_SELF="${LL_SELF}"
log "before: stale_commit=${B_STALE_COMMIT} lease_lost{stale_409}=${B_LL_409} {fenced_write_0_rows}=${B_LL_FENCED} {self_fence}=${B_LL_SELF}"

wait_delivered "${FREEZE_AFTER}"

IFS='|' read -r _ OLD_EPOCH OLD_SEQ OLD_STATE _ _ <<< "$(run_row "${RUN_ID}")"
[[ "${OLD_STATE}" == "RUNNING" ]] || die "run ${RUN_ID} is ${OLD_STATE} before the freeze; raise SINK_LATENCY_MS"

CONTAINER="$(container_of "${OWNER}")"
docker pause "${CONTAINER}" > /dev/null
PAUSED="${CONTAINER}"
log "paused ${OWNER} (container ${CONTAINER:0:12}) for ${FREEZE_SECONDS}s; its token is ${OLD_EPOCH}.${OLD_SEQ}"

sleep "${FREEZE_SECONDS}"

IFS='|' read -r MID_OWNER MID_EPOCH MID_SEQ MID_STATE _ _ <<< "$(run_row "${RUN_ID}")"
log "while frozen: run ${RUN_ID} is ${MID_STATE} on ${MID_OWNER:-?} with token ${MID_EPOCH}.${MID_SEQ}"
[[ "${MID_EPOCH}.${MID_SEQ}" != "${OLD_EPOCH}.${OLD_SEQ}" ]] \
    || warn "no takeover during the freeze (token unchanged); the frozen node may have held a row lock"

docker unpause "${CONTAINER}" > /dev/null
PAUSED=""
log "unpaused ${OWNER}"

wait_for "${RUN_TIMEOUT}" 1 "run ${RUN_ID} to reach SUCCEEDED" run_succeeded
log "run ${RUN_ID} SUCCEEDED"

# The brief asks for the stale-commit counter, but per PLAN section 11 the frozen node usually
# never reaches its commit: on wake-up it self-fences (self_fence), or an in-flight send gets 409
# (stale_409), or its next renew/checkpoint matches 0 rows (fenced_write_0_rows). Each of these
# stops the handler before commit. So the check is "stale commit OR any lease-lost reason rose",
# and all four counters are printed.
fenced() {
    read_counters
    ((STALE_COMMIT > B_STALE_COMMIT || LL_409 > B_LL_409 || LL_FENCED > B_LL_FENCED || LL_SELF > B_LL_SELF))
}
wait_for "${FENCE_WAIT}" 1 "${OWNER} to report a stale commit or a lost lease" fenced
log "after:  stale_commit=${STALE_COMMIT} lease_lost{stale_409}=${LL_409} {fenced_write_0_rows}=${LL_FENCED} {self_fence}=${LL_SELF}"

STATS="$(sink_run_stats "${RUN_ID}")"
STALE_REJECTED="$(jq -r '.staleRejected' <<< "${STATS}")"
if ((STALE_REJECTED > 0)); then
    log "OK: sink rejected ${STALE_REJECTED} stale-token request(s) for run ${RUN_ID}"
else
    # Zero is legitimate: a 409 needs a request carrying the old token to reach the sink after the
    # new owner's first send. When the pause lands between two sends (the common case), the frozen
    # node self-fences on wake-up and sends nothing, so the sink never sees the old token again.
    warn "sink staleRejected is 0 for run ${RUN_ID}: the freeze landed between sends and the node self-fenced"
fi

assert_exactly_once

printf '\nRESULT: PASS\n'
printf '  run                     %s (%s employees)\n' "${RUN_ID}" "${EMPLOYEES}"
printf '  frozen                  %s for %s s at token %s.%s\n' "${OWNER}" "${FREEZE_SECONDS}" "${OLD_EPOCH}" "${OLD_SEQ}"
printf '  stale commits           %s -> %s\n' "${B_STALE_COMMIT}" "${STALE_COMMIT}"
printf '  lease lost stale_409    %s -> %s\n' "${B_LL_409}" "${LL_409}"
printf '  lease lost fenced_write %s -> %s\n' "${B_LL_FENCED}" "${LL_FENCED}"
printf '  lease lost self_fence   %s -> %s\n' "${B_LL_SELF}" "${LL_SELF}"
printf '  sink staleRejected      %s\n' "${STALE_REJECTED}"
