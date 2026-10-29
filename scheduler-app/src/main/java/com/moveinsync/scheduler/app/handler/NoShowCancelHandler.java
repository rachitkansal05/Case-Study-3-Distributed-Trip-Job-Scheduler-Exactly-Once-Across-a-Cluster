package com.moveinsync.scheduler.app.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.core.domain.HandlerType;

/**
 * Cancels a trip after a no-show: one SMS effect telling the employee, key {@code runIdemKey}.
 *
 * <p>Job payload: {@code {"tripId": "T-1001", "employeeId": "e42"}} (both non-blank). Effect
 * payload: {@code {"event": "TRIP_CANCELLED_NO_SHOW", "tripId": ..., "employeeId": ..., "text": ...}}.
 */
public final class NoShowCancelHandler extends JobHandler<NoShowCancelHandler.Payload> {

    /**
     * Validated payload.
     *
     * @param tripId trip to cancel
     * @param employeeId employee who did not show up
     */
    public record Payload(String tripId, String employeeId) {}

    /** @param mapper shared Jackson mapper */
    public NoShowCancelHandler(ObjectMapper mapper) {
        super(mapper);
    }

    @Override
    public HandlerType type() {
        return HandlerType.NO_SHOW_CANCEL;
    }

    @Override
    protected Payload parsePayload(String payloadJson) {
        JsonNode root = readObject(payloadJson);
        return new Payload(requiredText(root, "tripId"), requiredText(root, "employeeId"));
    }

    @Override
    protected void execute(EffectContext ctx, Payload payload) {
        ctx.send(ctx.run().idemKey(), toJson(newObject()
                .put("event", "TRIP_CANCELLED_NO_SHOW")
                .put("tripId", payload.tripId())
                .put("employeeId", payload.employeeId())
                .put("text", "Your trip " + payload.tripId() + " was cancelled because you were marked as a no-show.")));
    }
}
