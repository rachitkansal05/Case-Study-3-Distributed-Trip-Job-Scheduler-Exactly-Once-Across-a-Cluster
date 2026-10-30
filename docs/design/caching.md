# Caching

> Design docs index: [README.md](README.md) · Full design: [architecture.md §14](architecture.md#caching-and-exactly-how-stale-it-can-get)

**Requirement:** cache hot data with a sensible eviction policy. Say exactly what can become
stale, for how long, and how staleness is bounded.

## 1. What is cached

Caffeine, in process on each node. Eviction is size-bounded (Caffeine's Window TinyLFU) plus,
where data can change, expire-after-write.

| Cache | Key and bound | How stale it can be | Why that is safe |
|---|---|---|---|
| Job definitions (engine, `RunExecutor`) | `(jobId, version)`, 50,000 entries | **Never** | A run records the job version it was created from, and a version never changes |
| `tenant-settings` (time zone) | tenant id, 10,000 entries, expire 5 s after write | Up to **5 s** | Only used to compute display and fire times |
| `next-fire-times` | `(jobId, version, count)`, 50,000 entries, expire 30 s after write | Up to about **35 s** after a tenant's time zone changes (5 s + 30 s) | A read-only API view; there is no API to change a zone yet |

## 2. What is never cached

Everything a guarantee depends on is read from PostgreSQL inside the claim transaction:

- leases, fencing tokens and due times;
- tenant slots: the cap is the slot rows themselves, so a cap change takes effect on the next
  claim;
- `tenant.paused`: read directly by every claim, so a pause takes effect on the next claim on
  every node.

`CachingIT` proves pause and cap changes are visible on the next claim, and that the cached views
expire as stated.

## 3. Why Caffeine and not Redis

What is cached is small (a time zone, a version-keyed definition, a computed list of instants),
bounded in staleness by a short TTL or by an immutable version, and needs no invalidation across
nodes. A shared Redis would add a network hop to every lookup and one more stateful service to
run, secure and fail over, to save a few cheap primary-key reads. The cost of the in-process
choice: each node warms its own cache (up to 3× the misses) and a zone change takes up to about
35 s to show in `next-fire-times`.

## 4. Observability

The two Spring caches export `cache_gets_total`, `cache_puts_total`, `cache_evictions_total` and
`cache_size`, shown on the Grafana dashboard.
