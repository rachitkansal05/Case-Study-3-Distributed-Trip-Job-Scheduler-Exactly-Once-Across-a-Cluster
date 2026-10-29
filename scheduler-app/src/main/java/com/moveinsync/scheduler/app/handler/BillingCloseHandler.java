package com.moveinsync.scheduler.app.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.core.domain.HandlerType;
import java.time.YearMonth;
import java.time.format.DateTimeParseException;

/**
 * Closes a billing period: one PUSH effect announcing the close, key {@code runIdemKey}.
 *
 * <p>Job payload: {@code {"period": "2026-10", "accountId": "ACME"}} ({@code yyyy-MM}, non-blank
 * account). Effect payload: {@code {"event": "BILLING_PERIOD_CLOSED", "period": ..., "accountId": ...}}.
 */
public final class BillingCloseHandler extends JobHandler<BillingCloseHandler.Payload> {

    /**
     * Validated payload.
     *
     * @param period billing month
     * @param accountId billed account
     */
    public record Payload(YearMonth period, String accountId) {}

    /** @param mapper shared Jackson mapper */
    public BillingCloseHandler(ObjectMapper mapper) {
        super(mapper);
    }

    @Override
    public HandlerType type() {
        return HandlerType.BILLING_CLOSE;
    }

    @Override
    protected Payload parsePayload(String payloadJson) {
        JsonNode root = readObject(payloadJson);
        String period = requiredText(root, "period");
        try {
            return new Payload(YearMonth.parse(period), requiredText(root, "accountId"));
        } catch (DateTimeParseException e) {
            throw invalid("field 'period' must be yyyy-MM");
        }
    }

    @Override
    protected void execute(EffectContext ctx, Payload payload) {
        ctx.send(ctx.run().idemKey(), toJson(newObject()
                .put("event", "BILLING_PERIOD_CLOSED")
                .put("period", payload.period().toString())
                .put("accountId", payload.accountId())));
    }
}
