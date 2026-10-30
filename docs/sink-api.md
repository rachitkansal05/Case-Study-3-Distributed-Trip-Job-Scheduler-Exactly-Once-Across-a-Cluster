# notification-sink HTTP API

`notification-sink` is the mock SMS/push provider that the scheduler calls for every side effect. Execution is at-least-once, so the same effect may be sent many times. The sink makes the *effect* exactly-once ([architecture.md](design/architecture.md) §02, §07 "Sink fencing"):

- **Idempotency key.** At most one delivery is recorded per `Idempotency-Key`. Repeats return `ALREADY_DELIVERED`.
- **Fencing token.** The sink keeps the highest `(epoch, seq)` it has accepted for each run. A request with a lower token is rejected with `409 STALE_TOKEN` and delivers nothing.
- **Attempt log.** Every validated request is recorded in `sink.attempt_log` with its outcome, so tests can show that duplicates were *attempted* but only one was *delivered* per key.

The scheduler-side client implements `com.moveinsync.scheduler.core.ports.EffectSink`. The client checklist is in [§8](#8-engine-client-checklist).

Base URL: `scheduler.sink.base-url` (default `http://localhost:8090`, env `SCHEDULER_SINK_BASE_URL`). The sink port is `SERVER_PORT`, default `8090`.

---

## 1. `POST /api/v1/deliveries`: deliver one effect

### Request

| Part | Value | Rules |
|---|---|---|
| `Content-Type` | `application/json` (a `charset` parameter is fine) | Anything else returns `415 UNSUPPORTED_MEDIA_TYPE`. |
| `Accept` | omit, or `application/json` | Success bodies are `application/json`; error bodies are `application/problem+json`. |
| `Idempotency-Key` | the run or item key, e.g. `77:emp-601` | **Required.** 1–256 characters, each visible ASCII (`0x21`–`0x7E`): no spaces, no control characters, no non-ASCII. Matched exactly (case-sensitive). |
| `Fencing-Token` | `<epoch>.<seq>`, e.g. `7.41` | **Required.** Exactly `FencingToken.toHeader()`: two plain non-negative decimals separated by one `.`, each fitting a signed 64-bit integer. No sign, spaces, or extra dots. Leading zeros are allowed (`007.41` = `7.41`). |

Body (JSON object; unknown fields are ignored):

```json
{
  "runId": 77,
  "channel": "SMS",
  "payload": { "to": "+91XXXXXXXXXX", "text": "Your cab arrives at 08:05" }
}
```

| Field | Type | Rules |
|---|---|---|
| `runId` | JSON integer | **Required**, `>= 1`. This is the run the fence is kept for. Strings (`"77"`), decimals (`77.0`) and booleans are rejected, never coerced. |
| `channel` | JSON string | **Required**, exactly `"SMS"` or `"PUSH"` (case-sensitive). |
| `payload` | JSON object | **Required**, any object (it can be `{}`). It is **never stored or logged**; the sink keeps only its SHA-256 (taken over the compact re-serialisation, keys in the order received). |

### Validation order

The sink checks each of these in turn and returns the first failure:

1. `Idempotency-Key` → `MISSING_IDEMPOTENCY_KEY` / `INVALID_IDEMPOTENCY_KEY`
2. `Fencing-Token` → `MISSING_FENCING_TOKEN` / `INVALID_FENCING_TOKEN`
3. body is a readable JSON object → `MALFORMED_BODY`
4. body fields → `VALIDATION_FAILED`, with an `errors` array that lists every bad field

A request that fails validation is answered `400` and **writes nothing**: no fence row, no attempt-log row, and it is not counted in `sink_deliveries_total`.

### Fault injection (before any write)

After validation, the sink applies any configured faults (see [§4](#4-admin-fault-injection)): it sleeps for `latencyMs + U[0, latencyJitterMs]`, then fails with probability `failureRate` (`500` or `503`, code `INJECTED_FAULT`). An injected failure **writes nothing**, so retrying it is always safe.

### What the sink does (one database transaction)

```
INSERT INTO sink.run_fence (run_id, max_epoch, max_seq) VALUES (:run, :e, :s) ON CONFLICT (run_id) DO NOTHING
SELECT max_epoch, max_seq FROM sink.run_fence WHERE run_id = :run FOR UPDATE      -- serialises the run
if (:e, :s) <  (max_epoch, max_seq):                       -- pair order: epoch first, then seq
    attempt_log ← STALE_TOKEN;  COMMIT;  → 409
if (:e, :s) >  (max_epoch, max_seq):  UPDATE run_fence SET (max_epoch, max_seq) = (:e, :s)
if (:e, :s) == (max_epoch, max_seq):  fence unchanged      -- retry by the current holder: allowed
INSERT INTO sink.delivery (...) ON CONFLICT (idem_key) DO NOTHING
    inserted  → attempt_log ← DELIVERED;          COMMIT; → 201
    conflict  → attempt_log ← ALREADY_DELIVERED;  COMMIT; → 200
```

Consequences you can rely on:

- **The fence check comes first.** A stale token gets `409` even when its key was already delivered. Only the current or a newer holder ever sees `200 ALREADY_DELIVERED`.
- **The fence is per run**, keyed by `runId` from the body. Tokens of different runs are never compared.
- **Equal tokens are accepted.** Any number of keys can be delivered under one token, and the same key can be retried under the same token.
- **Epoch beats seq.** `8.1` is newer than `7.999999`. Since `epoch = cluster_generation × 2^32 + timeline`, an epoch of `4294967297` (generation 1, timeline 1) is newer than any generation-0 epoch.
- **Idempotency keys are global, not per run.** If a key is already delivered (even by a different `runId`), the result is `200 ALREADY_DELIVERED`, and the response shows the original run and token. The payload is not compared: a repeat with a different payload is still `ALREADY_DELIVERED`.
- **A 409 is not a failure to retry.** It means a newer claim exists. Retrying with the same token will always get 409 again.

### Responses

**201 Created**: first delivery of this key; the effect happened now.

```http
HTTP/1.1 201
Content-Type: application/json

{"outcome":"DELIVERED","runId":77,"fencingToken":"7.41","deliveredAt":"2026-10-30T02:15:04.123456Z"}
```

**200 OK**: the key was delivered before; nothing was delivered now. The fields describe the **original** delivery: its `runId`, its token and its time.

```json
{"outcome":"ALREADY_DELIVERED","runId":77,"fencingToken":"7.41","deliveredAt":"2026-10-30T02:15:04.123456Z"}
```

**409 Conflict**: stale token; nothing was delivered. The attempt is logged.

```http
HTTP/1.1 409
Content-Type: application/problem+json

{"type":"about:blank","title":"Stale fencing token","status":409,
 "detail":"A higher fencing token has already been accepted for this run; nothing was delivered.",
 "code":"STALE_TOKEN","outcome":"STALE_TOKEN"}
```

**400 Bad Request**: see the [error codes](#6-errors). Example with field errors:

```json
{"type":"about:blank","title":"Validation failed","status":400,"detail":"The request body is invalid.",
 "code":"VALIDATION_FAILED","errors":[{"field":"channel","message":"must be SMS or PUSH"},
                                      {"field":"payload","message":"must be a JSON object"}]}
```

**500 / 503**: `INJECTED_FAULT` (500 or 503, as configured), `STORE_UNAVAILABLE` (503, sink DB unreachable), or `INTERNAL_ERROR` (500). The outcome of a `STORE_UNAVAILABLE` or `INTERNAL_ERROR` is unknown to the client: the transaction may or may not have committed. Retry with the **same key and token**. The idempotency key makes the retry safe.

`deliveredAt` is an ISO-8601 instant in UTC (sink DB `clock_timestamp()`, microsecond precision).

### Status → `DeliveryOutcome`

| Status | `code` / `outcome` | `DeliveryOutcome` | Client action |
|---|---|---|---|
| 201 | `outcome: DELIVERED` | `DELIVERED` | success |
| 200 | `outcome: ALREADY_DELIVERED` | `ALREADY_DELIVERED` | success (architecture.md §12: treat as delivered) |
| 409 | `code: STALE_TOKEN` | `STALE_TOKEN` | lease lost: stop the handler; not a job failure (`LeaseLostReason.STALE_TOKEN`) |
| 400 / 415 | any | none: throw (bug in the client or job data) | do not retry with the same request |
| 500 / 503, timeout, connection error | any | none: throw an unchecked exception | retryable; resend the same key and token |

### curl

```bash
curl -i -X POST http://localhost:8090/api/v1/deliveries \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: 77:emp-601' \
  -H 'Fencing-Token: 7.42' \
  -d '{"runId":77,"channel":"SMS","payload":{"to":"+91XXXXXXXXXX","text":"Cab at 08:05"}}'
# 201 the first time, 200 ALREADY_DELIVERED on repeats

curl -i -X POST http://localhost:8090/api/v1/deliveries \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: 77:emp-602' \
  -H 'Fencing-Token: 7.41' \
  -d '{"runId":77,"channel":"SMS","payload":{}}'
# 409 STALE_TOKEN: run 77 has already accepted 7.42
```

---

## 2. `GET /api/v1/deliveries/stats?runId={runId}`: per-run counters

`runId` must be an integer `>= 1` (otherwise `400 VALIDATION_FAILED`). An unknown run returns all zeros.

```json
{"runId":77,"deliveredDistinct":1200,"attempts":1249,"delivered":1200,"alreadyDelivered":48,"staleRejected":1}
```

| Field | Meaning |
|---|---|
| `deliveredDistinct` | rows in `sink.delivery` with this `run_id`: distinct keys delivered by this run |
| `attempts` | validated requests for this run (rows in `sink.attempt_log`) |
| `delivered` / `alreadyDelivered` / `staleRejected` | attempts by outcome; `attempts = delivered + alreadyDelivered + staleRejected` |

Validation failures (400) and injected faults are not attempts.

```bash
curl -s 'http://localhost:8090/api/v1/deliveries/stats?runId=77'
```

## 3. `GET /api/v1/deliveries/stats`: totals

```json
{"deliveredDistinct":10000,"attempts":10412,"delivered":10000,"alreadyDelivered":400,"staleRejected":12,
 "runs":10000,"maxDeliveriesPerKey":1}
```

`runs` is the number of runs with a fence row. **`maxDeliveriesPerKey` is the exactly-once proof**: it is `1` whenever anything has been delivered, and `0` when nothing has. The primary key on `idem_key` enforces it; the endpoint measures it independently. This call scans the tables, so it is meant for tests and demos.

### `GET /api/v1/fence/highest-epoch`: startup epoch guard (architecture.md F5)

```json
{"highestEpoch":7}
```

This is the highest `max_epoch` the sink has ever accepted for any run, or `0` when nothing has been accepted. Only accepted tokens raise a fence, so a rejected token never affects this value. A scheduler node whose current epoch is **lower** than this must refuse to start (`EffectSink.highestAcceptedEpoch()`).

```bash
curl -s http://localhost:8090/api/v1/fence/highest-epoch
```

---

## 4. Admin: fault injection

All `/admin/**` routes require `X-Admin-Token: <token>`. The token comes from `sink.admin.token`, env `SINK_ADMIN_TOKEN`, with the dev default `dev-only-sink-admin-token` (set it outside dev). A missing or wrong token returns `401 ADMIN_UNAUTHORIZED`.

Faults apply only to `POST /api/v1/deliveries`, after validation and before any database access. The read APIs and actuator are never faulted. The settings are held in memory per sink process: a restart clears them, and each instance must be configured separately.

### `PUT /admin/faults`

This replaces the whole setting. Every field is optional, and a missing field means no fault of that kind.

| Field | Range | Default |
|---|---|---|
| `latencyMs` | 0–60000 | 0 |
| `latencyJitterMs` | 0–60000 (adds uniform `[0, jitter]` ms) | 0 |
| `failureRate` | 0.0–1.0 | 0 |
| `failureStatus` | 500 or 503 | 503 |

The response is `200` with the effective settings. An out-of-range field returns `400 VALIDATION_FAILED` with `errors`, and the settings stay unchanged.

```bash
curl -i -X PUT http://localhost:8090/admin/faults \
  -H 'X-Admin-Token: dev-only-sink-admin-token' -H 'Content-Type: application/json' \
  -d '{"latencyMs":200,"latencyJitterMs":100,"failureRate":0.2,"failureStatus":503}'
# {"latencyMs":200,"latencyJitterMs":100,"failureRate":0.2,"failureStatus":503}
```

### `GET /admin/faults`

Returns the current settings (`200`, same shape as above).

### `DELETE /admin/faults`

Clears all faults. Returns `204`.

```bash
curl -i -X DELETE http://localhost:8090/admin/faults -H 'X-Admin-Token: dev-only-sink-admin-token'
```

---

## 5. Actuator

| Path | Purpose |
|---|---|
| `GET /actuator/health` (`/liveness`, `/readiness`) | health and probes |
| `GET /actuator/prometheus` | metrics, text format |

Metrics:

| Metric | Labels | Meaning |
|---|---|---|
| `sink_deliveries_total` | `outcome` = `DELIVERED`, `ALREADY_DELIVERED`, `STALE_TOKEN`, `INJECTED_FAULT`, `ERROR` | delivery requests that passed validation |
| `sink_delivery_latency_seconds` (histogram: `_bucket`, `_count`, `_sum`) | `outcome` | handling time inside the sink, including injected latency |

Every metric also carries `application="notification-sink"`.

## 6. Errors

Every error is an RFC 7807 ProblemDetail (`application/problem+json`) with `type` `about:blank`, `title`, `status`, `detail` and a stable **`code`**. Validation errors add `errors: [{field, message}]`. Bodies contain only fixed texts: no stack traces, no exception messages, no request path, and **never a request value** (no header value, payload, phone or e-mail). Branch on `code`, never on `detail`.

| Status | `code` | When |
|---|---|---|
| 409 | `STALE_TOKEN` | the token is lower than the run's fence; nothing delivered, attempt logged. Also has `outcome: "STALE_TOKEN"` |
| 400 | `MISSING_IDEMPOTENCY_KEY` | `Idempotency-Key` absent or empty |
| 400 | `INVALID_IDEMPOTENCY_KEY` | longer than 256, or not visible ASCII |
| 400 | `MISSING_FENCING_TOKEN` | `Fencing-Token` absent or empty |
| 400 | `INVALID_FENCING_TOKEN` | not `<epoch>.<seq>` with non-negative 64-bit decimals |
| 400 | `MALFORMED_BODY` | empty body, not JSON, or not a JSON object |
| 400 | `VALIDATION_FAILED` | bad body field or query parameter (`errors` lists them) |
| 401 | `ADMIN_UNAUTHORIZED` | `/admin/**` without a valid `X-Admin-Token` |
| 404 | `NOT_FOUND` | unknown path |
| 405 | `METHOD_NOT_ALLOWED` | wrong method on a known path |
| 406 | `NOT_ACCEPTABLE` | the `Accept` header excludes `application/json` |
| 415 | `UNSUPPORTED_MEDIA_TYPE` | body not sent as `application/json` |
| 500 / 503 | `INJECTED_FAULT` | fault injection; nothing written |
| 503 | `STORE_UNAVAILABLE` | the sink cannot reach its database; outcome unknown, so retry |
| 500 | `INTERNAL_ERROR` | anything else; outcome unknown, so retry |

## 7. Data and logging

- Tables live in schema `sink` and are migrated by the sink's own Flyway history (`sink.flyway_schema_history`):
  - `delivery(idem_key PK, run_id, fencing_epoch, fencing_seq, channel, payload_sha256, delivered_at)`
  - `run_fence(run_id PK, max_epoch, max_seq)`
  - `attempt_log(id, run_id, idem_key, fencing_epoch, fencing_seq, outcome, at)`
- Only the SHA-256 of the payload is stored. A CHECK constraint lets only 64 hex characters into that column.
- Each delivery log line contains `outcome`, `runId`, `idemKeyHash` (the first 16 hex characters of SHA-256 of the key) and `fencingToken`. The same fields are set as MDC fields, and the `json` profile writes them as JSON. The payload and the raw key are never logged.

## 8. Engine client checklist

For implementing `EffectSink` in `scheduler-app` (`repository.claim`):

1. **`send(runId, idemKey, token, payloadJson)`**:
   - Send `POST {base}/api/v1/deliveries` with `Content-Type: application/json`, `Idempotency-Key: idemKey` and `Fencing-Token: token.toHeader()`.
   - Send the body `{"runId": runId, "channel": "<SMS|PUSH>", "payload": <payloadJson>}`. `payloadJson` must be a JSON **object**; embed it as raw JSON, not as a quoted string.
   - Map 201 → `DELIVERED`, 200 → `ALREADY_DELIVERED`, and 409 → `STALE_TOKEN`. To be robust, also check that the 409 body has `code == "STALE_TOKEN"`.
   - Throw an unchecked exception on every other status, a timeout or an I/O error. The handler treats it as retryable. A retry must reuse the same key and token.
2. **Channel.** `EffectSink.send` has no channel parameter, but the body requires one. Pick it in the client: for example `SMS` for `PICKUP_ALERT`, or a `channel` field you put in the payload and read back. Any value other than `SMS` or `PUSH` is a 400.
3. **`highestAcceptedEpoch()`**: send `GET {base}/api/v1/fence/highest-epoch` and return `highestEpoch` (a JSON integer). Treat a non-200 as an error: the startup guard should fail closed rather than assume 0.
4. **Never log** `payloadJson`, phone numbers or e-mails. Log the key hash and the token only.
5. **Timeouts.** Use a request timeout shorter than the handler's remaining lease budget. Benchmarks may inject up to 120 s of latency (`latencyMs` + `latencyJitterMs`).
6. **Keys** must be visible ASCII and at most 256 characters. `jobId:scheduledAtUTC[:itemId]` with ISO-8601 instants fits.
