package com.moveinsync.scheduler.core.ports;

import com.moveinsync.scheduler.core.domain.DeliveryOutcome;
import com.moveinsync.scheduler.core.domain.FencingToken;

/**
 * Outbound side effect (the notification provider; plan sections 02, 07 and 12). Implemented in
 * {@code scheduler-app} as an HTTP client of {@code notification-sink}.
 *
 * <p>Contract of the sink, which makes the effect exactly-once while execution is at-least-once:
 *
 * <ul>
 *   <li>Every request carries {@code Idempotency-Key: idemKey} and {@code Fencing-Token:
 *       <epoch>.<seq>}.
 *   <li>The sink keeps the highest token per run; a request with a lower token is rejected
 *       ({@link DeliveryOutcome#STALE_TOKEN}, HTTP 409) and nothing is delivered.
 *   <li>Otherwise the first request per {@code idemKey} is delivered ({@link
 *       DeliveryOutcome#DELIVERED}); repeats return {@link DeliveryOutcome#ALREADY_DELIVERED}
 *       (HTTP 200) and deliver nothing.
 *   <li>Every attempt is logged in {@code sink.attempt_log}, whatever the outcome.
 * </ul>
 *
 * <p>Transport failures and timeouts are thrown as unchecked exceptions; the handler layer treats
 * them as retryable.
 */
public interface EffectSink {

    /**
     * Sends one effect.
     *
     * @param runId run the effect belongs to (fencing is tracked per run)
     * @param idemKey run or item idempotency key
     * @param token fencing token of the current claim
     * @param payloadJson effect body; may contain personal data, so implementations never log it
     * @return the sink's verdict
     */
    DeliveryOutcome send(long runId, String idemKey, FencingToken token, String payloadJson);

    /**
     * Highest fencing epoch the sink has ever accepted, or {@code 0} if none. Used by the startup
     * epoch guard: a node whose current epoch is lower must refuse to run (plan F5).
     */
    long highestAcceptedEpoch();
}
