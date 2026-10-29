package com.moveinsync.scheduler.app.handler;

import com.moveinsync.scheduler.core.domain.DeliveryOutcome;
import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.domain.RunContext;

/**
 * Everything a {@link JobHandler} may touch during one attempt. Implemented by the engine (one
 * instance per claimed run); handlers never see the claim protocol, the sink client or the lease
 * clock directly, so they cannot skip the fencing rules.
 *
 * <p>Rules the implementation enforces:
 *
 * <ul>
 *   <li>{@link #send} checks the self-fence ({@code LeaseClock.mayStartEffect()}) <b>before every
 *       effect</b> and throws {@link LeaseLostException} ({@code SELF_FENCE}) when it fails. A sink
 *       409 becomes {@link LeaseLostException} ({@code STALE_TOKEN}).
 *   <li>{@link #checkpoint} is a fenced write; 0 rows becomes {@link LeaseLostException} ({@code
 *       FENCED_WRITE_REJECTED}).
 *   <li>Sink transport failures surface as {@code RetryableJobException}, sink 400/415 as {@code
 *       PermanentJobException}.
 *   <li>When the lease is lost or the max run time passes, the worker thread is interrupted;
 *       handlers must let {@link InterruptedException} propagate.
 * </ul>
 */
public interface EffectContext {

    /** Ids, attempt, run idempotency key, fencing token, last checkpoint and deadline. */
    RunContext run();

    /** Handler type of the job (also decides the sink channel). */
    HandlerType handlerType();

    /** The job's payload JSON. May contain personal data: never log it. */
    String payloadJson();

    /** True while a new effect may still be started (lease not lost, self-fence deadline ahead). */
    boolean mayStartEffect();

    /**
     * Sends one effect under the run's fencing token.
     *
     * @param idemKey {@link RunContext#idemKey()} for single-effect handlers, {@link
     *     RunContext#itemKey(String)} for chunked ones
     * @param effectPayloadJson a JSON <b>object</b>, embedded raw as the sink body's {@code payload}
     * @return {@code DELIVERED} or {@code ALREADY_DELIVERED} (both success)
     * @throws LeaseLostException if self-fenced or the sink answered 409
     */
    DeliveryOutcome send(String idemKey, String effectPayloadJson);

    /**
     * Stores a progress checkpoint under the fenced predicate.
     *
     * @param checkpointJson JSON object without personal data, e.g. {@code {"nextItem":650}}
     * @throws LeaseLostException if the fenced write updated 0 rows
     */
    void checkpoint(String checkpointJson);
}
