# Job model, time zones and daylight saving time

> Design docs index: [README.md](README.md) · Full design: [architecture.md §05](architecture.md#05-data-model), [§08](architecture.md#08-recurring-jobs-time-zones-and-dst)

**Requirement:** one-off jobs at a time and recurring jobs from a cron expression, each with a
tenant, a payload, a priority and a maximum run time. Recurring jobs run in the tenant's time
zone, including zones with daylight saving time; define what happens to a 02:30 job on the day
clocks jump from 02:00 to 03:00, and on the day 01:30 happens twice.

## 1. Job types

| Field | One-off | Recurring |
|---|---|---|
| Schedule | `runAt` (an instant) | `cron`: five fields (`minute hour day-of-month month day-of-week`), evaluated in the tenant's time zone |
| Tenant | from the caller's token, never from the body | same |
| `handler` | one of the four job types below | same |
| `payload` | JSON object, ≤ 64 KiB | same |
| `priority` | 1 (most urgent) – 9; claims order by `priority, due_at` | same |
| `maxRunTime` | ISO-8601 duration; a hard stop (section 3) | same |
| `misfire`, `misfireThreshold` | `FIRE_NOW` / `SKIP` / `COALESCE`, default 60 s ([misfires](misfires-retries-and-dead-letters.md)) | same |
| `maxAttempts` | default 5, up to 100 | same |

In code, `JobDefinition` is a sealed interface with two records, `OneOffJob` and `RecurringJob`,
so every switch over job kinds is checked by the compiler.

| Handler | Payload | What it does |
|---|---|---|
| `PICKUP_ALERT` | `{"employeeIds":[…],"messageTemplate":"…{employeeId}…{pickupTime}…","pickupTime":"08:05"}` | One alert per employee, in checkpointed chunks of 50 |
| `ROSTER_GEN` | `{"shiftDate":"2026-11-01","siteId":"BLR-01"}` | Generates tomorrow's roster |
| `NO_SHOW_CANCEL` | `{"tripId":"T-1001","employeeId":"e42"}` | Auto-cancels a no-show |
| `BILLING_CLOSE` | `{"period":"2026-10","accountId":"ACME"}` | Closes daily billing |

The API only checks that the payload is a JSON object. A payload the handler cannot read is
dead-lettered on its first attempt as `INVALID_PAYLOAD`.

## 2. From a job to runs: the materializer

Each node's materializer runs every 5 s for the partitions it owns and creates `SCHEDULED`
`job_run` rows up to **15 minutes ahead**:

- A recurring job keeps a cursor (`job.next_fire_at`): the next occurrence not yet materialised.
  Each pass inserts occurrences from the cursor up to `now + 15 min`, at most 1,000 per job per
  pass, then advances the cursor with an optimistic update.
- `UNIQUE (job_id, scheduled_at)` makes materialisation idempotent: two nodes materialising the
  same partition during a handover insert each occurrence once.
- Each run copies `priority`, `max_run_time` and the job `version` it came from, so a cached job
  definition keyed by `(jobId, version)` can never be stale.

## 3. Max run time is a hard stop

- At `maxRunTime` the handler is interrupted and further effects are refused; the attempt fails
  with `TIMEOUT` and goes through the normal retries.
- A handler that returns anyway is still a `TIMEOUT`, never a success.
- A handler that ignores the interrupt is abandoned one renew interval (3 s) later: the engine
  records `TIMEOUT` with the run's fenced commit, which frees its slot, and stops renewing its
  lease. Java cannot stop a thread, so that thread keeps one worker until it returns.

## 4. Time zones and DST

Cron expressions match **local wall time in the tenant's zone**. They are parsed by our own
`CronSchedule` (Vixie cron day matching: if both day-of-month and day-of-week are restricted,
either may match). `WallClockResolver` then maps each local time to exactly one instant.

| Case | Example (America/New_York) | Rule | Fires at |
|---|---|---|---|
| Clocks jump 02:00 → 03:00 (gap) | 8 Mar 2026, job at 02:30 | Fires **once at 03:00 local**, when the skipped hour ends, as classic cron does. Java's default would give 03:30 | 07:00Z |
| 01:00–01:59 happens twice (overlap) | 1 Nov 2026, job at 01:30 | Fires **once, at the first 01:30** (EDT, −04:00) | 05:30Z |
| Sub-hourly job in the repeated hour | `*/15 * * * *` | Each distinct local time fires once: 4 runs in the repeated hour, not 8 | — |
| Zone without DST | Asia/Kolkata, 22:00 | Direct mapping | 16:30Z |

**Why these rules.** The gap rule keeps the job from being lost (it still runs that day) and
fires it as close as possible to the intended time. The overlap rule keeps it from running twice;
the first occurrence is the one people expect. The run's idempotency key uses the resolved UTC
instant, so a re-materialisation can never create a second run for the same wall time.

Tenant zones must be IANA region IDs (`Asia/Kolkata`, `America/New_York`); fixed offsets are
rejected because they cannot express DST.

`GET /api/v1/jobs/{id}/next-fire-times?count=N` returns each instant with its local time and
offset, so the DST behaviour is visible through the API.

## 5. Tests

| Test | What it proves |
|---|---|
| `JobDefinitionTest`, `JobValidationIT` | Both job kinds, required fields, payload and cron validation, error codes |
| `CronScheduleTest` | Five-field parsing, Vixie day matching, invalid expressions |
| `WallClockResolverTest` | Gap → 03:00, overlap → first 01:30, zones without DST |
| `NextFireTimesTest` | The 2026 transitions in New York, sub-hourly jobs in the repeated hour |
| `MaterializerIT` | Idempotent materialisation, cursor, 15-minute horizon, bounded catch-up |
| `MaxRunTimeIT` | Interrupt at max run time, `TIMEOUT` retry, hard stop of a handler that ignores the interrupt |
