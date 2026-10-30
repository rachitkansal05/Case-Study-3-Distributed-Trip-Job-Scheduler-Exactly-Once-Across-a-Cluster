# Patroni production example

A PostgreSQL 16 cluster that meets the failover preconditions F1–F5 of [architecture.md §12](../../docs/design/architecture.md#failover-requirements-production) (also in [failure-handling.md](../../docs/design/failure-handling.md#6-database-failover-production)). The docker compose demo at the repository root runs a single Postgres and makes no claim to survive a database failover. This directory shows the setup the scheduler expects in production.

> **Not run here.** The machine this was written on has no Docker. The YAML files were parsed with PyYAML (`yaml.safe_load`): `docker-compose.patroni.yml` and `patroni.yml` both parse. The shell scripts pass `bash -n`. `haproxy.cfg` and the Dockerfile have not been validated (no `haproxy -c`, no build). Treat the first real start as the test.

| File | What it is |
|---|---|
| `docker-compose.patroni.yml` | 3 × etcd 3.5, 3 × Patroni + PostgreSQL 16 (`pg-1/2/3`), HAProxy |
| `Dockerfile` | `FROM postgres:16` plus `pip install "patroni[etcd3,psycopg3]==4.0.4"` in a virtualenv. We build our own small image rather than use a third-party Patroni image, so the Postgres binaries are the official ones. |
| `patroni.yml` | One template for all three members. Per-node values come from `PATRONI_NAME`, `PATRONI_RESTAPI_CONNECT_ADDRESS` and `PATRONI_POSTGRESQL_CONNECT_ADDRESS` |
| `entrypoint.sh` | Copies the config, applies `WATCHDOG_MODE`, warns if the watchdog is missing, then `exec patroni` |
| `post-bootstrap.sh` | Patroni `post_bootstrap` hook. Creates role and database `scheduler` once, on the first primary |
| `haproxy.cfg` | `:5000` read-write (Patroni `GET /primary` = 200), `:5001` read-only (`GET /replica` = 200), `:7000` stats |

## Run it

```bash
sudo modprobe softdog              # provides /dev/watchdog (Linux host)
sudo chown 999 /dev/watchdog       # uid 999 = postgres in the image
docker compose -f deploy/patroni/docker-compose.patroni.yml up -d --build
docker exec pg-1 patronictl -c /tmp/patroni.yml list
```

**Single laptop.** Only one process can hold `/dev/watchdog`, and Patroni opens it only while it is leader. With three members on one host, a hung old leader keeps the device open, and under `mode: required` no other member can become leader. For a one-host try-out, start with `WATCHDOG_MODE=automatic docker compose … up`. On a host without `/dev/watchdog` at all (macOS, Windows, WSL without softdog), also remove the `devices:` entry, because Docker refuses to start a container whose device mapping is missing. `automatic` uses the watchdog when it can and runs without it otherwise, so it drops the hardware half of F4. Production uses `required` with one member per machine.

## Point the schedulers at it

Two options.

**1. Multi-host JDBC URL (no proxy in the data path):**

```
SPRING_DATASOURCE_URL=jdbc:postgresql://pg-1:5432,pg-2:5432,pg-3:5432/scheduler?targetServerType=primary&target_session_attrs=read-write&hostRecheckSeconds=2&connectTimeout=5&socketTimeout=30
```

- **`targetServerType=primary`** is the property the **Java driver (pgjdbc) honours**. pgjdbc tries the hosts in order and keeps only one that is not read-only (it checks `transaction_read_only` / `in_hot_standby`). `hostRecheckSeconds` controls how long it caches a host's role.
- **`target_session_attrs=read-write`** is the **libpq** name for the same rule (psql, psycopg, pgbench, `pg_isready`). pgjdbc does not use it: it ignores unknown URL parameters. It stays in the URL because the requirement (architecture.md §12 F4, "clients connect with `target_session_attrs=read-write`") is written in libpq terms, and because ops tools that reuse the connection string honour it. Do not remove `targetServerType=primary`. That is the parameter that protects the scheduler.
- The libpq form, for psql: `postgresql://scheduler@pg-1:5432,pg-2:5432,pg-3:5432/scheduler?target_session_attrs=read-write`.
- pgjdbc checks the role only when a connection opens. A connection that is already open to a demoted primary is cut when Patroni restarts that Postgres as a replica. Hikari then replaces it, and the new connection lands on the new primary.

**2. Through HAProxy (`:5000`):**

```
SPRING_DATASOURCE_URL=jdbc:postgresql://haproxy:5000/scheduler?targetServerType=primary&target_session_attrs=read-write
```

HAProxy forwards only to the member whose Patroni says it holds the leader lock. `on-marked-down shutdown-sessions` cuts every open session to a member as soon as it fails the check. The parameters stay in the URL as a second guard.

In both cases the scheduler and the sink must be able to resolve `pg-N`/`haproxy`. Run both stacks in one compose project, or attach both to a shared external network. Replace the toxiproxy URLs from the demo compose file.

## F1–F5 → settings

Line numbers are for the files in this directory as committed.

| ID | Requirement | Setting(s) | Where |
|---|---|---|---|
| F1 | Every acknowledged commit is on at least one standby | `synchronous_commit: "on"`: the commit returns only after the synchronous standby has flushed the WAL | `patroni.yml:70` |
| F2 | The primary never falls back to async | `synchronous_mode: true`, `synchronous_mode_strict: true`: with no synchronous standby, Patroni sets `synchronous_standby_names = '*'` and writes block | `patroni.yml:48-49` |
| F3 | Only a replica holding every acknowledged commit is promoted | `synchronous_node_count: 1` with 3 members = 1 primary + 2 replicas, 1 synchronous (`FIRST 1`). Patroni writes the current synchronous standby to the etcd `/sync` key and promotes only that member. Extra guards: `maximum_lag_on_failover: 1048576`, `check_timeline: true`, tags `nofailover: false`, `failover_priority: 1`, `nosync: false` | `patroni.yml:53`, `:37`, `:40`, `:130-134` |
| F4 | The old primary can't accept writes after a promotion | Leader lock `ttl: 30`, `loop_wait: 10`, `retry_timeout: 10` (a primary that cannot renew the lock demotes itself). `failsafe_mode: false` (losing etcd means demote). `watchdog.mode: required` (a hung Patroni gets its host reset before the lock expires) plus the `/dev/watchdog` device. HAProxy `/primary` check with `shutdown-sessions`. Clients use `targetServerType=primary` / `target_session_attrs=read-write` | `patroni.yml:31-33`, `:63`, `:123-124`; `docker-compose.patroni.yml:45`; `haproxy.cfg:39-41`; scheduler JDBC URL above |
| F5 | Fencing epochs strictly increase | Every promotion starts a new timeline. `use_pg_rewind: true` + `wal_log_hints: "on"` let the old primary rejoin on the new timeline instead of forking. `cluster_generation` covers rebuilds, and the startup check rejects an epoch below the sink's highest (`EpochGuard`, `FencingEpochReader` in `scheduler-app`) | `patroni.yml:66`, `:75`; below |

`synchronous_standby_names` is **managed by Patroni** and must not be set by hand. With `synchronous_node_count: 1`, Patroni writes a single name (for example `"pg-2"`), which Postgres treats as `FIRST 1 (pg-2)`. With N > 1 it writes `N (a,b,…)`. If you switch to `synchronous_mode: quorum` (`ANY k (…)`), F3 also needs the architecture.md §12 rule: promotion must reach n − k + 1 standbys and pick the highest flushed WAL position, or refuse.

## Fencing epoch and `cluster_generation`

The scheduler's token epoch is `cluster_generation × 2^32 + timeline` (architecture.md §07). The timeline is the first 8 hex digits of `pg_walfile_name(pg_current_wal_lsn())`.

- **Failover or switchover:** Patroni promotes a replica, Postgres starts timeline T+1, so every new claim's epoch is one higher than any token issued by the old primary. The sink rejects the old tokens with 409. Nothing has to be done by hand.
- **Rebuild from a fresh `initdb`** (cluster wiped, restored from a logical dump, new etcd scope): the timeline restarts at 1, so the epoch would go *down*. The schedulers then refuse to start: the startup check sees an epoch below the sink's `GET /api/v1/fence/highest-epoch`. Raise the generation on the new primary before starting them:

  ```sql
  UPDATE cluster_meta SET cluster_generation = cluster_generation + 1;
  SELECT cluster_generation FROM cluster_meta;
  ```

  One step is enough: an epoch of generation g+1 beats every epoch of generation g at any timeline. A restore from a physical backup (pgBackRest, `pg_basebackup`) keeps the timeline history and needs no change.

## Verify

```bash
docker exec pg-1 patronictl -c /tmp/patroni.yml list           # Leader / Sync Standby / Replica, TL column = timeline
docker exec pg-1 patronictl -c /tmp/patroni.yml show-config    # synchronous_mode, strict, node count
curl -s localhost:8008/primary -o /dev/null -w '%{http_code}\n'  # 200 only on the leader (8009, 8010 for pg-2/3)
PGPASSWORD=scheduler psql -h localhost -p 5000 -U scheduler -d scheduler <<'SQL'
SHOW synchronous_standby_names;                    -- e.g. "pg-2"; set by Patroni
SHOW synchronous_commit;                           -- on
SELECT pg_walfile_name(pg_current_wal_lsn());      -- first 8 hex digits = timeline
SELECT application_name, state, sync_state FROM pg_stat_replication;   -- one 'sync', one 'potential'
SELECT cluster_generation FROM cluster_meta;       -- after the scheduler's Flyway migration ran
SQL
```

## Failover drill

1. Run a load (or `chaos/kill-during-alerts.sh` with the schedulers pointed here) and note the timeline: `SELECT pg_walfile_name(pg_current_wal_lsn())`.
2. **Planned:** `docker exec pg-1 patronictl -c /tmp/patroni.yml switchover --leader <current> --candidate <sync standby> --force`. **Unplanned:** `docker exec pg-1 patronictl -c /tmp/patroni.yml failover --candidate <sync standby> --force`, or `docker kill pg-1` when pg-1 is the leader.
3. Expect: `patronictl list` shows the former sync standby as Leader with TL + 1. Writes pause for about `ttl` (≤ 30 s) on a kill, or a few seconds on a switchover. HAProxy `:5000` moves to the new leader. The scheduler logs connection errors, then resumes. New tokens carry the higher epoch.
4. Check: no run lost or doubled (`GET /api/v1/deliveries/stats` → `maxDeliveriesPerKey == 1`), and the old member rejoins as a replica via pg_rewind (`docker start pg-1`, then `patronictl list`).
5. **Strict mode:** stop both replicas (`docker stop pg-2 pg-3`). Writes on the primary must hang, not succeed (`synchronous_standby_names = '*'`). Start one replica again and the writes complete.
