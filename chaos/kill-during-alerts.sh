#!/usr/bin/env bash
# Chaos: kill -9 the node that is sending 1,200 pickup alerts, then prove exactly-once delivery.
#
#   1. mint dev tokens, create a tenant and a one-off PICKUP_ALERT job for $EMPLOYEES employees
#   2. trigger it (Idempotency-Key), find the owning node in job_run.owner_node
#   3. once the sink shows $KILL_AFTER deliveries, `docker kill -s KILL` that node's container
#   4. wait until another node holds the run with a new fencing token, then until SUCCEEDED
#   5. assert sink deliveredDistinct == $EMPLOYEES and maxDeliveriesPerKey == 1
#   6. print the takeover time (DB clock), restart the killed service (unless --no-restart)
#
# Usage: chaos/kill-during-alerts.sh [--no-restart]
# Env:   see chaos/lib.sh and chaos/README.md; KILL_AFTER (default 300).
set -euo pipefail

# shellcheck source=chaos/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

RESTART=1
for arg in "$@"; do
    case "${arg}" in
        --no-restart) RESTART=0 ;;
        -h | --help)
            sed -n '2,14p' "${BASH_SOURCE[0]}"
            exit 0
            ;;
        *) die "unknown argument '${arg}' (use --no-restart)" ;;
    esac
done

KILL_AFTER="${KILL_AFTER:-300}"
KILLED=""

cleanup() {
    local status=$?
    sink_clear_faults
    disable_job
    if [[ -n "${KILLED}" && "${RESTART}" == "1" ]]; then
        log "restarting ${KILLED}"
        compose start "${KILLED}" > /dev/null || warn "could not restart ${KILLED}"
    elif [[ -n "${KILLED}" ]]; then
        log "--no-restart: ${KILLED} left stopped (docker compose start ${KILLED})"
    fi
    exit "${status}"
}
trap cleanup EXIT

require_tools
cd "${REPO_ROOT}"

setup_alert_run "chaos-kill"
((KILL_AFTER < EMPLOYEES)) || die "KILL_AFTER (${KILL_AFTER}) must be below EMPLOYEES (${EMPLOYEES})"

wait_delivered "${KILL_AFTER}"

IFS='|' read -r OLD_OWNER OLD_EPOCH OLD_SEQ OLD_STATE OLD_LEASE _ <<< "$(run_row "${RUN_ID}")"
[[ "${OLD_STATE}" == "RUNNING" ]] || die "run ${RUN_ID} is ${OLD_STATE} before the kill; raise SINK_LATENCY_MS"
[[ "${OLD_OWNER}" == "${OWNER}" ]] || die "owner moved from ${OWNER} to ${OLD_OWNER} before the kill"

CONTAINER="$(container_of "${OWNER}")"
KILL_DB_TIME="$(db_now)"
docker kill -s KILL "${CONTAINER}" > /dev/null
KILLED="${OWNER}"
log "killed ${OWNER} (container ${CONTAINER:0:12}) at DB time ${KILL_DB_TIME}; token was ${OLD_EPOCH}.${OLD_SEQ}"

# Takeover = another claim of the same run with a different token. The token is the reliable
# signal: owner_node alone could repeat if the killed node came back under the same name.
NEW_OWNER=""
NEW_EPOCH=""
NEW_SEQ=""
CLAIM_DB_TIME=""
token_changed() {
    local owner epoch seq state lease now
    IFS='|' read -r owner epoch seq state lease now <<< "$(run_row "${RUN_ID}")"
    [[ -n "${seq}" && "${epoch}.${seq}" != "${OLD_EPOCH}.${OLD_SEQ}" ]] || return 1
    NEW_OWNER="${owner}"
    NEW_EPOCH="${epoch}"
    NEW_SEQ="${seq}"
    # The claim sets lease_until = clock_timestamp() + lease. We poll every 0.5 s and renewals
    # run every 3 s, so on first sight lease_until - lease is the claim instant (DB clock).
    if [[ "${state}" == "RUNNING" && -n "${lease}" ]]; then
        CLAIM_DB_TIME="$(awk -v l="${lease}" -v s="${LEASE_SECONDS}" 'BEGIN { printf "%.3f", l - s }')"
    else
        CLAIM_DB_TIME="${now}"
    fi
}
wait_for 60 0.5 "run ${RUN_ID} to be claimed with a new token" token_changed

[[ "${NEW_OWNER}" != "${OWNER}" ]] || warn "new owner is ${NEW_OWNER} again (container restarted by its restart policy?)"
TAKEOVER="$(awk -v a="${KILL_DB_TIME}" -v b="${CLAIM_DB_TIME}" 'BEGIN { printf "%.2f", b - a }')"
SINCE_EXPIRY="$(awk -v a="${OLD_LEASE}" -v b="${CLAIM_DB_TIME}" 'BEGIN { printf "%.2f", b - a }')"
log "run ${RUN_ID} taken over by ${NEW_OWNER} with token ${NEW_EPOCH}.${NEW_SEQ}"
if awk -v t="${TAKEOVER}" 'BEGIN { exit !(t > 11.2) }'; then
    warn "takeover took ${TAKEOVER} s, above the 11.2 s budget of PLAN section 11 (reported, not failed)"
fi

wait_for "${RUN_TIMEOUT}" 1 "run ${RUN_ID} to reach SUCCEEDED" run_succeeded
log "run ${RUN_ID} SUCCEEDED"

assert_exactly_once

printf '\nRESULT: PASS\n'
printf '  run              %s (%s employees)\n' "${RUN_ID}" "${EMPLOYEES}"
printf '  killed           %s at token %s.%s\n' "${OWNER}" "${OLD_EPOCH}" "${OLD_SEQ}"
printf '  new owner        %s at token %s.%s\n' "${NEW_OWNER}" "${NEW_EPOCH}" "${NEW_SEQ}"
printf '  takeover time    %s s (kill -> new claim, DB clock; budget 11.2 s)\n' "${TAKEOVER}"
printf '  after expiry     %s s (old lease_until -> new claim)\n' "${SINCE_EXPIRY}"
