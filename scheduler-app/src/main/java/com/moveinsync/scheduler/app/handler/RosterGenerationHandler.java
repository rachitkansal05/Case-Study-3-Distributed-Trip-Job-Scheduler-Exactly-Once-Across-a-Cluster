package com.moveinsync.scheduler.app.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.core.domain.HandlerType;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;

/**
 * Nightly roster regeneration: one PUSH effect announcing the roster, key {@code runIdemKey}.
 *
 * <p>Job payload: {@code {"shiftDate": "2026-11-01", "siteId": "BLR-01"}} (ISO date, non-blank
 * site). Effect payload: {@code {"event": "ROSTER_READY", "shiftDate": ..., "siteId": ...}}.
 */
public final class RosterGenerationHandler extends JobHandler<RosterGenerationHandler.Payload> {

    /**
     * Validated payload.
     *
     * @param shiftDate shift date
     * @param siteId site id
     */
    public record Payload(LocalDate shiftDate, String siteId) {}

    /** @param mapper shared Jackson mapper */
    public RosterGenerationHandler(ObjectMapper mapper) {
        super(mapper);
    }

    @Override
    public HandlerType type() {
        return HandlerType.ROSTER_GEN;
    }

    @Override
    protected Payload parsePayload(String payloadJson) {
        JsonNode root = readObject(payloadJson);
        String date = requiredText(root, "shiftDate");
        try {
            return new Payload(LocalDate.parse(date), requiredText(root, "siteId"));
        } catch (DateTimeParseException e) {
            throw invalid("field 'shiftDate' must be an ISO date (yyyy-MM-dd)");
        }
    }

    @Override
    protected void execute(EffectContext ctx, Payload payload) {
        ctx.send(ctx.run().idemKey(), toJson(newObject()
                .put("event", "ROSTER_READY")
                .put("shiftDate", payload.shiftDate().toString())
                .put("siteId", payload.siteId())));
    }
}
