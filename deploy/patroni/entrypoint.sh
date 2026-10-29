#!/usr/bin/env bash
# Starts Patroni with /etc/patroni/patroni.yml (mounted read-only).
# WATCHDOG_MODE=automatic|off overrides `watchdog.mode: required` for single-host demos.
set -euo pipefail

src=/etc/patroni/patroni.yml
cfg=/tmp/patroni.yml

if [[ -n "${WATCHDOG_MODE:-}" ]]; then
    case "${WATCHDOG_MODE}" in
        required | automatic | off) ;;
        *) echo "WATCHDOG_MODE must be required, automatic or off" >&2; exit 1 ;;
    esac
    sed -E "s/^([[:space:]]+mode:)[[:space:]]+required/\1 ${WATCHDOG_MODE}/" "${src}" > "${cfg}"
else
    cp "${src}" "${cfg}"
fi

mode="$(sed -nE 's/^[[:space:]]+mode:[[:space:]]+([a-z]+).*/\1/p' "${cfg}" | head -n 1)"
if [[ "${mode}" == "required" && ! -w /dev/watchdog ]]; then
    echo "WARNING: watchdog mode is 'required' but /dev/watchdog is missing or not writable by $(id -un);" \
         "this member will never become leader. Load softdog on the host and map the device." >&2
fi

exec patroni "${cfg}"
