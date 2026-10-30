# Security: authentication, roles and tenant isolation

> Design docs index: [README.md](README.md) · Full design: [architecture.md §13](architecture.md#13-api-and-access-control)

**Requirement:** robust authentication and role-based access (employee vs admin vs operator),
with tenant isolation so one client can never read another client's data. No personal data in
logs or error responses.

## 1. Authentication

- Stateless **HS256 JWTs**, validated by Spring Security's OAuth2 resource server
  (`JwtSecurityConfig`). Claims: `sub`, the tenant, and `roles`.
- The key is `SCHEDULER_JWT_HMAC_SECRET` (at least 32 characters). `application.yml` falls back
  to a public dev key; **a node outside the `dev` and `test` profiles refuses to start while the
  secret is still that default**, so a deployment that forgot the variable fails loudly instead
  of accepting tokens anyone can sign (`JwtSecretGuardTest`).
- `POST /dev/token` mints demo tokens and exists only in the `dev` profile (`DevTokenIT`,
  `DevTokenDisabledIT`). In production, tokens would come from an identity provider.
- The sink's fault-injection API requires `X-Admin-Token` (`SINK_ADMIN_TOKEN`).

Order of checks: no or invalid token → `401`; wrong role → `403` (reveals nothing about data);
right role but another tenant's resource → `404`.

## 2. Roles

| Role | Can | Cannot |
|---|---|---|
| EMPLOYEE | Read schedules (job, next fire times) of their own tenant | See payloads, change anything |
| ADMIN | Manage their own tenant's jobs, pause, resume, cap; list its dead letters | Touch another tenant; create tenants |
| OPERATOR | Platform on-call: create tenants, work across tenants on operational data (states, counts, error codes), replay dead letters, view the cluster | See payloads |

The role matrix is enforced per method and path and tested endpoint by endpoint (`RoleMatrixIT`).

## 3. Tenant isolation

- The tenant comes **only from the token**, never from the request body or path.
- Every repository query takes a `TenantScope` and adds a tenant predicate, so another tenant's
  row is simply not found: a request for another tenant's job, tenant or dead letter returns
  **`404`, not `403`**, and changes nothing (`TenantIsolationIT`).
- Idempotency keys are namespaced per tenant; operators have their own namespace.

Isolation is enforced in the application, in every query. Postgres row-level security would be
a second line of defence; it is not used.

## 4. No personal data in logs or errors

- Payloads (phone numbers, emails, home locations) are never logged and never returned to
  non-admins (`payloadRedacted: true`).
- Error responses never echo submitted values ([error handling](error-handling.md)).
- As a last line of defence, JSON logs mask every string value through `PiiValueMasker`, and
  plain-text logs go through `PiiMaskingConverter` (`PiiMaskingTest`, `LoggingIT`).
- Log lines carry `idemKeyHash`, never the raw idempotency key.
- The notification sink stores only a SHA-256 of each payload and logs only a key hash
  (`PayloadPrivacyIT`).

## 5. Compose is a laptop setup

`docker-compose.yml` runs every node with the `dev` profile, so `/dev/token` is open on ports
8081–8083 and mints an OPERATOR or ADMIN token for any tenant without authentication. Never
expose the stack on a shared network. Outside a laptop, drop `dev` from `SPRING_PROFILES_ACTIVE`
and set `SCHEDULER_JWT_HMAC_SECRET` and `SINK_ADMIN_TOKEN` to real secrets.

## 6. Tests

| Test | What it proves |
|---|---|
| `RoleMatrixIT` | Every endpoint × role; 401 vs 403 vs 404; tokens signed with another key rejected |
| `TenantIsolationIT` | Tenant B can never read or change tenant A's jobs, runs, tenant or dead letters |
| `JwtSecretGuardTest` | The public default secret is refused outside `dev`/`test` |
| `DevTokenIT`, `DevTokenDisabledIT` | `/dev/token` only in the `dev` profile |
| `PiiMaskingTest`, `LoggingIT` | No phone, email or coordinate from a payload reaches the logs |
| `ErrorResponseTest` | No stack trace, SQL or payload value in any error response |
| `PayloadPrivacyIT` | The sink stores only payload hashes and never logs personal data |
