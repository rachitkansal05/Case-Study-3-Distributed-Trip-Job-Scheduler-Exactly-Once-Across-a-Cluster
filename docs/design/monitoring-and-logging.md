# Monitoring: metrics, alerts and structured logging

> Design docs index: [README.md](README.md) · Full design: [architecture.md §14](architecture.md#14-metrics-logging-caching-and-errors) · On-call: [`docs/RUNBOOK.md`](../RUNBOOK.md)

**Requirement:** fire lag (actual start minus scheduled time) as a histogram, plus running,
retrying and dead-lettered counts. Expose what an on-call engineer needs at 3 AM: lag, error
rate, rejections, retries, queue depth. Structured logging.

## 1. Where to look

| What | Where |
|---|---|
| Metrics | `/actuator/prometheus` on every node and on the sink; Prometheus scrapes all four every 5 s |
| Dashboard | Grafana "Trip Job Scheduler": http://localhost:3000/d/scheduler-overview (fire lag, runs and outcomes, claims and partitions, leases and takeovers, dead letters by error code, tenant caps, the sink, JVM/Hikari/caches) |
| Alerts | **13 rules** in [`ops/prometheus/alerts.yml`](../../ops/prometheus/alerts.yml), firing list at http://localhost:9090/alerts (no Alertmanager in the demo) |
| What to do | Every alert has a `runbook_url` to its section of [`docs/RUNBOOK.md`](../RUNBOOK.md) |

## 2. Metrics

| Metric | What on-call learns | Alert |
|---|---|---|
| `scheduler_fire_lag_seconds` (histogram, SLO buckets 0.1–30 s, by `handler`, `tenant_tier`) | **Fire lag**: actual start − scheduled time, both on the DB clock, recorded at a run's first start | `SchedulerFireLagHigh`: p99 > 2 s for 5 min |
| `scheduler_runs{state}` (`running`, `retrying`, `dead_lettered`) | **Running, retrying and dead-lettered counts** (DB counts, every 5 s) | `SchedulerDeadLettersIncreasing`: any new dead letter |
| `scheduler_queue_depth` | **Queue depth**: runs due but not started | `SchedulerQueueDepthGrowing`: rising for 2 min |
| `scheduler_run_outcome_total{outcome}` | **Error rate**: success, retryable, permanent, timeout | `SchedulerErrorRateHigh`: > 5 % for 5 min |
| `scheduler_tenant_cap_rejections_total{tenant}` | **Rejections**: claims held back by tenant caps | informational |
| `scheduler_stale_commit_rejected_total` | Fenced commits that matched nothing (GC pauses, network splits) | `SchedulerStaleCommitsRejected`: any increase |
| `scheduler_lease_lost_total{reason}` | `stale_409`, `fenced_write_0_rows`, `self_fence` | `SchedulerLeaseLostSpike`: > 5 for one reason in 5 min |
| `scheduler_dead_letters_total{error_code}` | Dead letters by error code | `SchedulerFencingConflictDeadLetters`: any `FENCING_CONFLICT` in 15 min |
| `scheduler_lease_renew_failures_total` | A node losing its leases | `SchedulerLeaseRenewFailures`: any in 2 min |
| `scheduler_orphan_takeovers_total`, `scheduler_orphan_takeover_time_seconds` | Takeovers, and the time from the dead owner's last renewal to the restart (normally 10–10.7 s) | `SchedulerOrphanTakeoverSlow`: p99 > 11.2 s |
| `scheduler_partitions_owned{node}` | Partition spread | `SchedulerPartitionsUnowned`: total ≠ 128 for 10 s |
| `scheduler_claim_batch_seconds` | Claim transaction latency | `SchedulerClaimBatchSlow`: p99 > 100 ms for 5 min |
| `scheduler_tenant_cap{tenant,kind}` | Configured vs effective cap | `SchedulerTenantCapDraining`: effective > configured for 30 min |
| `up{job=~"scheduler\|notification-sink"}` | A node or the sink not scraped | `SchedulerTargetDown`: 30 s |
| `sink_deliveries_total{outcome}`, `sink_delivery_latency_seconds` | What the provider saw: delivered, already delivered, stale token, injected fault | — |

The takeover histogram is measured on the DB clock from the dead owner's last successful renewal
(expired `lease_until` minus the 10 s lease, the last moment it was proven alive) to the takeover
claim. That is exactly the interval the 10.7 s bound limits, so the 11.2 s alert compares like
with like.

## 3. Structured logging

- JSON lines (profile `json`, used by compose), through Logback with a PII-masking JSON encoder.
- Request lines carry `traceId` and `tenantId`; the trace id is also returned in `X-Trace-Id`
  and in every error body.
- Lines written while a run executes carry `nodeId`, `runId`, `jobId`, `tenantId`,
  `fencingToken`, `attempt` and `idemKeyHash` (a hash, never the raw key).
- No payload value is ever logged; string values are masked as a last line of defence
  ([security §4](security-and-tenant-isolation.md#4-no-personal-data-in-logs-or-errors)).

## 4. Keeping it consistent

[`ops/metric-names.txt`](../../ops/metric-names.txt) lists every metric name. Tests check the
alert rules, the dashboard and the runbook against it, and prove each name is really exported:

| Test | What it proves |
|---|---|
| `AlertRulesTest` | 13 rules, valid PromQL shape, only known metrics, each with a runbook link that exists |
| `GrafanaDashboardTest` | Dashboard JSON uses only known metrics, has the `node` and `tenant` variables |
| `RunbookTest` | One runbook section per alert |
| `MetricsIT`, `OpsMetricNamesIT` | Every listed metric is exported with its tags |
| `EngineMetricsTest` | Histogram buckets, counters and gauges of the engine |
| `LoggingIT` | JSON fields present; no phone, email or coordinate in the output |
| `ComposeConfigTest` | Compose wires profiles, scraping and dashboards as documented |
