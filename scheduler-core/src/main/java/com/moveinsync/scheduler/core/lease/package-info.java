/**
 * Monotonic self-fencing lease clock ({@code LeaseClock}), plan section 07. The
 * {@code FencingToken} value type itself lives in {@code core.domain}. Owned by the Core team.
 *
 * <p>The self-fence is the first of three layers that stop a stale node: it only uses the local
 * monotonic clock, starts before the claim is sent, and is backed by sink fencing and DB fencing.
 */
package com.moveinsync.scheduler.core.lease;
