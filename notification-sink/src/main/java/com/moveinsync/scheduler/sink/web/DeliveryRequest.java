package com.moveinsync.scheduler.sink.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.moveinsync.scheduler.sink.domain.Channel;
import com.moveinsync.scheduler.sink.web.SinkProblemException.FieldViolation;
import java.util.ArrayList;
import java.util.List;

/**
 * Body of {@code POST /api/v1/deliveries}. Unknown fields are ignored.
 *
 * @param runId run the effect belongs to; required, {@code >= 1}
 * @param channel {@code "SMS"} or {@code "PUSH"}; required
 * @param payload JSON object with the message; required. Never stored or logged, only hashed.
 */
public record DeliveryRequest(Long runId, String channel, JsonNode payload) {

    /** Rule violations, empty when the body is valid. Messages never contain the rejected value. */
    public List<FieldViolation> violations() {
        List<FieldViolation> out = new ArrayList<>();
        if (runId == null) {
            out.add(new FieldViolation("runId", "is required"));
        } else if (runId < 1) {
            out.add(new FieldViolation("runId", "must be >= 1"));
        }
        if (channel == null) {
            out.add(new FieldViolation("channel", "is required"));
        } else if (Channel.of(channel).isEmpty()) {
            out.add(new FieldViolation("channel", "must be SMS or PUSH"));
        }
        if (payload == null || payload.isNull()) {
            out.add(new FieldViolation("payload", "is required"));
        } else if (!payload.isObject()) {
            out.add(new FieldViolation("payload", "must be a JSON object"));
        }
        return out;
    }

    /** The channel; call only after {@link #violations()} returned empty. */
    public Channel channelValue() {
        return Channel.of(channel).orElseThrow();
    }

    /** Never prints the payload. */
    @Override
    public String toString() {
        return "DeliveryRequest[runId=" + runId + ", channel=" + channel + ", payload=<redacted>]";
    }
}
