#!/usr/bin/env bash
# Patroni post_bootstrap hook: runs once on the first primary, with a superuser connection URL
# as $1. Creates the role and database the scheduler and notification-sink use.
set -euo pipefail

url="$1"
password="${SCHEDULER_DB_PASSWORD:-scheduler}"

psql "${url}" -v ON_ERROR_STOP=1 -v pw="${password}" <<'SQL'
CREATE ROLE scheduler LOGIN PASSWORD :'pw';
CREATE DATABASE scheduler OWNER scheduler;
SQL
