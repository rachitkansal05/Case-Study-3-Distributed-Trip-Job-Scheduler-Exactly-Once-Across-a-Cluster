# shellcheck shell=bash
# Shared helpers for the chaos scripts. Sourced, never executed directly.
#
# Callers set `set -euo pipefail` before sourcing. Everything is overridable by environment:
#
#   SCHEDULER_URLS   "scheduler-1=http://localhost:8081,scheduler-2=http://localhost:8082,scheduler-3=http://localhost:8083"
#   SINK_URL         http://localhost:8090
#   SINK_ADMIN_TOKEN dev-only-sink-admin-token
#   COMPOSE_FILE     if set, docker compose reads it itself; otherwise the scripts pass
#                    -f docker-compose.yml -f chaos/docker-compose.chaos.yml
#   PSQL             command that runs psql against the scheduler DB, read as words
#                    (default: docker compose <args> exec -T postgres psql -U scheduler -d scheduler)
#   EMPLOYEES        number of employees in the PICKUP_ALERT payload (1200)
#   SINK_LATENCY_MS  latency injected into the sink so the run lasts long enough to hit (25)
#   RUN_TIMEOUT      seconds to wait for the run to finish (300)

CHAOS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${CHAOS_DIR}/.." && pwd)"

SCHEDULER_URLS="${SCHEDULER_URLS:-scheduler-1=http://localhost:8081,scheduler-2=http://localhost:8082,scheduler-3=http://localhost:8083}"
SINK_URL="${SINK_URL:-http://localhost:8090}"
SINK_ADMIN_TOKEN="${SINK_ADMIN_TOKEN:-dev-only-sink-admin-token}"
EMPLOYEES="${EMPLOYEES:-1200}"
SINK_LATENCY_MS="${SINK_LATENCY_MS:-25}"
RUN_TIMEOUT="${RUN_TIMEOUT:-300}"

# Lease length used by the claim (application.yml scheduler.run-lease.duration). Used only to turn
# lease_until back into the claim instant for the takeover-time report.
LEASE_SECONDS="${LEASE_SECONDS:-10}"

if [[ -n "${COMPOSE_FILE:-}" ]]; then
    COMPOSE_ARGS=()
else
    COMPOSE_ARGS=(-f "${REPO_ROOT}/docker-compose.yml" -f "${CHAOS_DIR}/docker-compose.chaos.yml")
fi

if [[ -n "${PSQL:-}" ]]; then
    read -r -a PSQL_CMD <<< "${PSQL}"
else
    PSQL_CMD=(docker compose "${COMPOSE_ARGS[@]}" exec -T postgres psql -U scheduler -d scheduler)
fi

# ---------------------------------------------------------------- output

log() {
    printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2
}

die() {
    printf '[%s] FAIL: %s\n' "$(date -u +%H:%M:%S)" "$*" >&2
    exit 1
}

warn() {
    printf '[%s] WARN: %s\n' "$(date -u +%H:%M:%S)" "$*" >&2
}

require_tools() {
    local tool
    for tool in curl jq docker awk; do
        command -v "${tool}" > /dev/null 2>&1 || die "required tool '${tool}' is not installed"
    done
    docker compose version > /dev/null 2>&1 || die "'docker compose' (v2) is not available"
}

# ---------------------------------------------------------------- compose / docker

compose() {
    docker compose "${COMPOSE_ARGS[@]}" "$@"
}

# container_of SERVICE -> container id (fails if the service has no running container)
container_of() {
    local service="$1" id
    id="$(compose ps -q "${service}")"
    [[ -n "${id}" ]] || die "no container found for compose service '${service}'"
    printf '%s\n' "${id}"
}

# ---------------------------------------------------------------- scheduler URLs

# scheduler_url SERVICE -> base URL of that node
scheduler_url() {
    local want="$1" entry name url
    local -a entries
    IFS=',' read -r -a entries <<< "${SCHEDULER_URLS}"
    for entry in "${entries[@]}"; do
        name="${entry%%=*}"
        url="${entry#*=}"
        if [[ "${name}" == "${want}" ]]; then
            printf '%s\n' "${url%/}"
            return 0
        fi
    done
    die "service '${want}' is not listed in SCHEDULER_URLS (${SCHEDULER_URLS})"
}

# scheduler_services -> one service name per line
scheduler_services() {
    local entry
    local -a entries
    IFS=',' read -r -a entries <<< "${SCHEDULER_URLS}"
    for entry in "${entries[@]}"; do
        printf '%s\n' "${entry%%=*}"
    done
}

# api_url_excluding SERVICE -> base URL of any other node (for API calls while SERVICE is down)
api_url_excluding() {
    local skip="$1" name
    while IFS= read -r name; do
        if [[ "${name}" != "${skip}" ]]; then
            scheduler_url "${name}"
            return 0
        fi
    done < <(scheduler_services)
    die "no scheduler node other than '${skip}' in SCHEDULER_URLS"
}

first_api_url() {
    scheduler_url "$(scheduler_services | head -n 1)"
}

# ---------------------------------------------------------------- HTTP

# http METHOD URL [BODY] [curl header args...]
# Prints the response body; fails with status and body on a non-2xx answer.
http() {
    local method="$1" url="$2" body="${3:-}"
    shift 3 || shift $#
    local out status payload
    local -a args=(-sS -X "${method}" --max-time 15 -w '\n%{http_code}' -H 'Accept: application/json')
    if [[ -n "${body}" ]]; then
        args+=(-H 'Content-Type: application/json' --data-binary "${body}")
    fi
    out="$(curl "${args[@]}" "$@" "${url}")" || die "${method} ${url}: connection failed"
    status="${out##*$'\n'}"
    payload="${out%$'\n'*}"
    if [[ "${status}" != 2* ]]; then
        die "${method} ${url} returned HTTP ${status}: ${payload}"
    fi
    printf '%s\n' "${payload}"
}

# mint_token TENANT ROLE -> JWT (POST /dev/token exists only with the dev Spring profile)
mint_token() {
    local tenant="$1" role="$2" body resp
    body="$(jq -cn --arg t "${tenant}" --arg r "${role}" --arg s "chaos-${role,,}" \
        '{tenant: $t, role: $r, subject: $s}')"
    resp="$(http POST "$(first_api_url)/dev/token" "${body}")" \
        || die "POST /dev/token failed: is the 'dev' profile active? (see chaos/README.md)"
    jq -er '.token' <<< "${resp}" || die "no token in /dev/token response"
}

# ---------------------------------------------------------------- database

# psql_q SQL -> unaligned, tuples-only output, '|' separated
psql_q() {
    "${PSQL_CMD[@]}" -v ON_ERROR_STOP=1 -Atqc "$1"
}

db_now() {
    psql_q "SELECT extract(epoch FROM clock_timestamp())"
}

# run_row RUN_ID -> owner_node|fencing_epoch|fencing_seq|state|lease_until(epoch s)|db now(epoch s)
run_row() {
    psql_q "SELECT coalesce(owner_node,''), coalesce(fencing_epoch::text,''), coalesce(fencing_seq::text,''),
                   state, coalesce(extract(epoch FROM lease_until)::text,''), extract(epoch FROM clock_timestamp())
              FROM job_run WHERE id = $1"
}

# ---------------------------------------------------------------- sink

sink_run_stats() {
    http GET "${SINK_URL%/}/api/v1/deliveries/stats?runId=$1"
}

sink_totals() {
    http GET "${SINK_URL%/}/api/v1/deliveries/stats"
}

sink_set_latency() {
    local ms="$1"
    http PUT "${SINK_URL%/}/admin/faults" "$(jq -cn --argjson ms "${ms}" '{latencyMs: $ms}')" \
        -H "X-Admin-Token: ${SINK_ADMIN_TOKEN}" > /dev/null
}

sink_clear_faults() {
    curl -sS -o /dev/null -X DELETE --max-time 10 -H "X-Admin-Token: ${SINK_ADMIN_TOKEN}" \
        "${SINK_URL%/}/admin/faults" || warn "could not clear sink faults"
}

# ---------------------------------------------------------------- metrics

# prom_sum BASE_URL METRIC [LABEL_REGEX] -> sum of all samples of METRIC whose label set matches
# LABEL_REGEX (default: all series). Prints 0 if the metric is absent.
prom_sum() {
    local base="$1" metric="$2" filter="${3:-}" text
    text="$(curl -sS --max-time 10 "${base%/}/actuator/prometheus")" \
        || die "could not scrape ${base}/actuator/prometheus"
    awk -v m="${metric}" -v f="${filter}" '
        $0 ~ /^#/ { next }
        {
            name = $1; labels = ""
            i = index(name, "{")
            if (i > 0) { labels = substr($0, i, index($0, "}") - i + 1); name = substr(name, 1, i - 1) }
            if (name != m) next
            if (f != "" && labels !~ f) next
            sum += $NF
        }
        END { printf "%.0f\n", sum + 0 }' <<< "${text}"
}

# ---------------------------------------------------------------- waiting

# wait_for TIMEOUT_S INTERVAL_S DESCRIPTION CMD... : retries CMD until it succeeds or the time is up
wait_for() {
    local timeout="$1" interval="$2" what="$3"
    shift 3
    local deadline=$((SECONDS + timeout))
    until "$@"; do
        if ((SECONDS >= deadline)); then
            die "timed out after ${timeout}s waiting for: ${what}"
        fi
        sleep "${interval}"
    done
}

# ---------------------------------------------------------------- scenario setup

# Globals set by setup_alert_run, read by the scenario scripts.
TENANT=""
OP_TOKEN=""
ADMIN_TOKEN=""
JOB_ID=""
RUN_ID=""
OWNER=""

employee_ids_json() {
    jq -cn --argjson n "$1" '[range(1; $n + 1) | "emp-" + (("0000" + tostring) | .[-4:])]'
}

# setup_alert_run PREFIX: tenant + one-off PICKUP_ALERT job with $EMPLOYEES ids, manual trigger,
# then waits until the run is RUNNING and records its owner node.
setup_alert_run() {
    local prefix="$1" api body resp run_at state
    api="$(first_api_url)"
    TENANT="${prefix}-$(date +%s)"

    OP_TOKEN="$(mint_token "platform" OPERATOR)"
    ADMIN_TOKEN="$(mint_token "${TENANT}" ADMIN)"

    # POST /api/v1/tenants is OPERATOR only (JwtSecurityConfig); body is CreateTenantRequest.
    body="$(jq -cn --arg id "${TENANT}" '{id: $id, timeZone: "Asia/Kolkata", cap: 4}')"
    http POST "${api}/api/v1/tenants" "${body}" -H "Authorization: Bearer ${OP_TOKEN}" > /dev/null
    log "tenant ${TENANT} created"

    # The job's own run_at is a day away, so the only run that executes now is the manual trigger.
    run_at="$(psql_q "SELECT to_char((now() + interval '1 day') AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')")"

    # POST /api/v1/jobs is ADMIN only; body is CreateJobRequest; payload per PickupAlertHandler.
    body="$(jq -cn --arg runAt "${run_at}" --argjson ids "$(employee_ids_json "${EMPLOYEES}")" '{
        kind: "ONE_OFF", handler: "PICKUP_ALERT", runAt: $runAt, priority: 1,
        maxRunTime: "PT15M", maxAttempts: 5,
        payload: {employeeIds: $ids, messageTemplate: "Chaos test: cab for {employeeId}"}}')"
    resp="$(http POST "${api}/api/v1/jobs" "${body}" -H "Authorization: Bearer ${ADMIN_TOKEN}" \
        -H "Idempotency-Key: ${TENANT}-create")"
    JOB_ID="$(jq -er '.id' <<< "${resp}")"
    log "job ${JOB_ID} created (${EMPLOYEES} employees)"

    sink_set_latency "${SINK_LATENCY_MS}"
    log "sink latency set to ${SINK_LATENCY_MS} ms per delivery"

    # POST /api/v1/jobs/{id}/trigger (ADMIN or OPERATOR) needs an Idempotency-Key; 202 TriggerView.
    resp="$(http POST "${api}/api/v1/jobs/${JOB_ID}/trigger" "" \
        -H "Authorization: Bearer ${ADMIN_TOKEN}" -H "Idempotency-Key: ${TENANT}-trigger")"
    RUN_ID="$(jq -er '.runId' <<< "${resp}")"
    log "triggered run ${RUN_ID}"

    local deadline=$((SECONDS + 60))
    while :; do
        resp="$(http GET "${api}/api/v1/jobs/${JOB_ID}/runs" "" -H "Authorization: Bearer ${ADMIN_TOKEN}")"
        state="$(jq -r --argjson id "${RUN_ID}" '.items[] | select(.runId == $id) | .state' <<< "${resp}")"
        [[ "${state}" == "RUNNING" ]] && break
        case "${state}" in
            SUCCEEDED) die "run ${RUN_ID} finished before it could be disrupted; raise SINK_LATENCY_MS" ;;
            DEAD_LETTERED | CANCELLED) die "run ${RUN_ID} ended in ${state}" ;;
        esac
        ((SECONDS < deadline)) || die "run ${RUN_ID} did not reach RUNNING within 60s (state '${state}')"
        sleep 0.5
    done

    # RunView does not expose the owner, so read it from job_run. Node ids equal service names.
    OWNER="$(psql_q "SELECT owner_node FROM job_run WHERE id = ${RUN_ID}")"
    [[ -n "${OWNER}" ]] || die "run ${RUN_ID} is RUNNING but has no owner_node"
    scheduler_url "${OWNER}" > /dev/null
    log "run ${RUN_ID} is RUNNING on ${OWNER}"
}

# wait_delivered N: waits until the sink shows at least N distinct deliveries for the run
delivered_at_least() {
    local n
    n="$(sink_run_stats "${RUN_ID}" | jq -r '.deliveredDistinct')"
    ((n >= $1))
}

wait_delivered() {
    wait_for 120 0.2 "sink to show $1 deliveries for run ${RUN_ID}" delivered_at_least "$1"
    log "sink shows $(sink_run_stats "${RUN_ID}" | jq -r '.deliveredDistinct') delivered for run ${RUN_ID}"
}

run_succeeded() {
    [[ "$(psql_q "SELECT state FROM job_run WHERE id = ${RUN_ID}")" == "SUCCEEDED" ]]
}

# assert_exactly_once: deliveredDistinct == EMPLOYEES for the run and maxDeliveriesPerKey == 1
assert_exactly_once() {
    local stats totals distinct maxper
    stats="$(sink_run_stats "${RUN_ID}")"
    totals="$(sink_totals)"
    log "sink run stats: ${stats}"
    log "sink totals:    ${totals}"
    distinct="$(jq -r '.deliveredDistinct' <<< "${stats}")"
    maxper="$(jq -r '.maxDeliveriesPerKey' <<< "${totals}")"
    [[ "${distinct}" == "${EMPLOYEES}" ]] \
        || die "deliveredDistinct for run ${RUN_ID} is ${distinct}, expected ${EMPLOYEES}"
    [[ "${maxper}" == "1" ]] || die "maxDeliveriesPerKey is ${maxper}, expected 1 (a key was delivered twice)"
    log "OK: ${distinct} distinct deliveries, maxDeliveriesPerKey = 1"
}

# disable_job: DELETE /api/v1/jobs/{id} (ADMIN) so the day-away one-off never fires
disable_job() {
    [[ -n "${JOB_ID}" && -n "${ADMIN_TOKEN}" ]] || return 0
    curl -sS -o /dev/null -X DELETE --max-time 10 -H "Authorization: Bearer ${ADMIN_TOKEN}" \
        "$(api_url_excluding "${OWNER:-none}")/api/v1/jobs/${JOB_ID}" || warn "could not disable job ${JOB_ID}"
}
