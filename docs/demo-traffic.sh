#!/usr/bin/env bash
# Demo traffic for Grafana (used by docs/DEMO.md step 3). Creates real jobs through
# the API so the dashboard has fire lag, throughput, running counts and tenant caps to show:
#   - tenant demo-steady: STEADY one-off pickup alerts spread over the next SPREAD seconds,
#     plus one recurring job that fires every minute
#   - optional (BURST > 0): tenant demo-big (cap 4) gets BURST alerts all due at +BURST_AT s and
#     queues at its cap, while tenant demo-small's 5 alerts start at once (fairness). Queued runs
#     are late by design, so they raise the combined fire-lag p99: use it only to show fairness.
#
# Usage: bash docs/demo-traffic.sh            (stack must be up; run it 2-3 minutes before you
#                                              open Grafana on camera)
# Env:   STEADY (default 300), SPREAD (seconds, default 300), BURST (default 0), BURST_AT (default 90)
set -euo pipefail

API=localhost:8081
STEADY="${STEADY:-300}"
SPREAD="${SPREAD:-300}"
BURST="${BURST:-0}"
BURST_AT="${BURST_AT:-90}"

tok() {
    curl -s -X POST "$API/dev/token" -H 'Content-Type: application/json' \
        -d "{\"tenant\":\"$1\",\"role\":\"$2\"}" | jq -r .token
}
at() { date -u -d "+$1 seconds" +%FT%TZ; }   # GNU date (Git Bash, Linux)

tenant() {   # id, cap
    curl -s -o /dev/null -X POST "$API/api/v1/tenants" -H "Authorization: Bearer $OP" \
        -H 'Content-Type: application/json' -d "{\"id\":\"$1\",\"timeZone\":\"Asia/Kolkata\",\"cap\":$2}"
}
alert() {    # token, runAt, employee count, tag
    local emps
    emps=$(seq -f "\"$4-%03g\"" 1 "$3" | paste -sd, -)
    curl -s -o /dev/null -w '%{http_code}' -X POST "$API/api/v1/jobs" -H "Authorization: Bearer $1" \
        -H 'Content-Type: application/json' \
        -d "{\"kind\":\"ONE_OFF\",\"handler\":\"PICKUP_ALERT\",\"runAt\":\"$2\",\"maxRunTime\":\"PT2M\",
             \"payload\":{\"employeeIds\":[$emps],\"messageTemplate\":\"Cab at {pickupTime}\",\"pickupTime\":\"08:05\"}}"
}

command -v jq > /dev/null || { echo "jq is missing"; exit 1; }
OP=$(tok platform OPERATOR)
[[ -n "$OP" && "$OP" != null ]] || { echo "could not get a token: is the stack up (docker compose ps)?"; exit 1; }

tenant demo-steady 32
tenant demo-big 4
tenant demo-small 32
STEADY_T=$(tok demo-steady ADMIN); BIG_T=$(tok demo-big ADMIN); SMALL_T=$(tok demo-small ADMIN)

echo "recurring job every minute (tenant demo-steady)"
curl -s -o /dev/null -X POST "$API/api/v1/jobs" -H "Authorization: Bearer $STEADY_T" -H 'Content-Type: application/json' \
    -d '{"kind":"RECURRING","handler":"ROSTER_GEN","cron":"* * * * *","payload":{"shiftDate":"2026-11-01","siteId":"BLR-01"},"maxRunTime":"PT1M"}'

if ((BURST > 0)); then
    echo "burst: $BURST alerts for demo-big (cap 4) and 5 for demo-small, all due at +${BURST_AT}s"
    due=$(at "$BURST_AT")
    for ((i = 1; i <= BURST; i++)); do alert "$BIG_T" "$due" 1 "big$i" > /dev/null; done
    for ((i = 1; i <= 5; i++)); do alert "$SMALL_T" "$due" 1 "small$i" > /dev/null; done
fi

echo "steady: $STEADY alerts for demo-steady over the next ${SPREAD}s"
ok=0
for ((i = 1; i <= STEADY; i++)); do
    code=$(alert "$STEADY_T" "$(at $((10 + i * SPREAD / STEADY)))" $((1 + i % 5)) "emp$i")
    [[ "$code" == 201 ]] && ok=$((ok + 1))
done
echo "created $ok/$STEADY steady jobs. Open Grafana in about a minute (time range: last 15 minutes, refresh 5s)."
