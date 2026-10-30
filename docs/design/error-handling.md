# Error and exception handling

> Design docs index: [README.md](README.md) · Full design: [architecture.md §14](architecture.md#error-handling)

**Requirement:** a robust error-handling framework with meaningful, non-leaking error messages.
No personal data (phone, email, home location) in logs or error responses.

## 1. One error format

Every error is an RFC 7807 problem detail (`application/problem+json`) with a stable `code`, a
safe `detail` and a `traceId` (also sent in the `X-Trace-Id` header):

```json
{
  "type": "about:blank",
  "title": "Not Found",
  "status": 404,
  "detail": "Job not found",
  "instance": "urn:trace:4f1c2a9e7b3d4c5a8e2f1b0c9d8e7f6a",
  "code": "JOB_NOT_FOUND",
  "traceId": "4f1c2a9e7b3d4c5a8e2f1b0c9d8e7f6a"
}
```

Validation errors (`400 VALIDATION_FAILED`, `INVALID_CRON`) add `errors: [{field, reason}]` and
never echo the submitted value.

## 2. One exception hierarchy, one handler

- `SchedulerException` is a **sealed** class; each subclass carries its HTTP status and code:
  `NotFoundException` (404), `ForbiddenException` (403), `ConflictException` (409, for example
  `IDEMPOTENCY_KEY_REUSED`, `DEAD_LETTER_ALREADY_REPLAYED`, `TENANT_EXISTS`),
  `ValidationException` (400), `StoreUnavailableException` (503).
- `ProblemDetailAdvice` maps them, plus Spring's own errors (bad JSON, wrong method, missing
  header), in one place. Security errors (401/403) use the same format through
  `ProblemSecurityHandlers`.
- **Anything unexpected** is logged (masked) and answered with a generic `500 INTERNAL_ERROR`
  that contains only the trace id.
- A database that cannot be reached returns `503 SCHEDULER_STORE_UNAVAILABLE`, so a client knows
  to retry with the same `Idempotency-Key`.

## 3. Errors inside job runs

Handler failures are classified, not just logged:

| Class | Examples | Effect |
|---|---|---|
| `RetryableJobException` | `SINK_5XX`, `SINK_TIMEOUT`, `TIMEOUT` | retry with backoff, then dead letter |
| `PermanentJobException` | `INVALID_PAYLOAD`, `SINK_REJECTED_<status>` | dead letter at once |
| `LeaseLostException` | `409 STALE_TOKEN`, fenced write matched 0 rows | not a failure: the attempt is given back |

A run's `last_error` stores only the error code and a safe message (at most 500 characters),
never payload data. Details: [misfires, retries and dead letters](misfires-retries-and-dead-letters.md#2-retries).

## 4. Nothing leaks

- Responses never contain stack traces, SQL or payload values.
- Logs never contain payload values; JSON logs mask every string value and plain-text logs are
  masked by `PiiMaskingConverter` and `PiiMaskingThrowableConverter` (exception messages too).
- The sink stores only a SHA-256 of each payload.

## 5. Tests

| Test | What it proves |
|---|---|
| `ErrorResponseTest` | Every error type: status, code, trace id; no stack trace, SQL or value |
| `JobValidationIT` | Field-level validation errors without echoing values |
| `RoleMatrixIT` | 401 / 403 / 404 in the problem format |
| `PiiMaskingTest`, `LoggingIT`, `PayloadPrivacyIT` | No personal data in logs, errors or the sink |
