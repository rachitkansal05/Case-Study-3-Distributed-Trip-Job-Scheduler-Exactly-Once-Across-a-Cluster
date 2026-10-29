/**
 * Misfire policies (sealed: {@code FireNow}, {@code Skip}, {@code Coalesce}), backoff ({@code
 * ExponentialFullJitterBackoff}), failure classification and the retry / dead-letter decision,
 * plan section 09. Owned by the Core team.
 *
 * <p>Everything here is pure: no clock reads, no I/O. Callers pass the authoritative time in.
 * Error codes and safe messages produced here never contain payload data, and never the message
 * text of an unknown exception.
 */
package com.moveinsync.scheduler.core.policy;
