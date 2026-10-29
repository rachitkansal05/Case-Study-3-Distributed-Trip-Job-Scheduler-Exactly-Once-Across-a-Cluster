package com.moveinsync.scheduler.app.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.app.engine.support.FakeEffectSink;
import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.policy.PermanentJobException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class PickupAlertHandlerTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final FakeEffectSink sink = new FakeEffectSink();
    private final PickupAlertHandler handler = new PickupAlertHandler(mapper);

    private RecordingEffectContext ctx(String payload) {
        return RecordingEffectContext.of(HandlerType.PICKUP_ALERT, payload, null, sink);
    }

    @Test
    void sendsOneSmsPerEmployeeWithRenderedText() throws Exception {
        RecordingEffectContext ctx = ctx("""
                {"employeeIds":["e1","e2"],"messageTemplate":"Hi {employeeId}, your cab arrives at {pickupTime}",
                 "pickupTime":"08:05"}""");
        handler.handle(ctx);

        assertThat(handler.type()).isEqualTo(HandlerType.PICKUP_ALERT);
        assertThat(ctx.sends()).extracting(RecordingEffectContext.Sent::idemKey).containsExactly("idem:e1", "idem:e2");
        JsonNode first = mapper.readTree(ctx.sends().get(0).payloadJson());
        assertThat(first.get("employeeId").asText()).isEqualTo("e1");
        assertThat(first.get("text").asText()).isEqualTo("Hi e1, your cab arrives at 08:05");
        assertThat(ctx.checkpoints()).containsExactly("{\"nextItem\":2}");
    }

    @Test
    void pickupTimeOptionalWhenTemplateDoesNotUseIt() throws Exception {
        RecordingEffectContext ctx = ctx("{\"employeeIds\":[\"e1\"],\"messageTemplate\":\"Cab assigned\"}");
        handler.handle(ctx);
        assertThat(ctx.sends()).hasSize(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "",
        "not-json SECRET-9876",
        "[\"SECRET-9876\"]",
        "{\"messageTemplate\":\"x\"}",
        "{\"employeeIds\":\"SECRET-9876\",\"messageTemplate\":\"x\"}",
        "{\"employeeIds\":[],\"messageTemplate\":\"x\"}",
        "{\"employeeIds\":[\"   \"],\"messageTemplate\":\"x\"}",
        "{\"employeeIds\":[42],\"messageTemplate\":\"x\"}",
        "{\"employeeIds\":[\"SECRET-9876\",\"SECRET-9876\"],\"messageTemplate\":\"x\"}",
        "{\"employeeIds\":[\"SECRET 9876\"],\"messageTemplate\":\"x\"}",
        "{\"employeeIds\":[\"SECRET-é9876\"],\"messageTemplate\":\"x\"}",
        "{\"employeeIds\":[\"SECRET-9876\"]}",
        "{\"employeeIds\":[\"SECRET-9876\"],\"messageTemplate\":\"at {pickupTime}\"}",
        "{\"employeeIds\":[\"SECRET-9876\"],\"messageTemplate\":\"x\"} trailing"
    })
    void invalidPayloadsAreRejectedWithoutEchoingValues(String payload) {
        RecordingEffectContext ctx = ctx(payload);
        assertThatThrownBy(() -> handler.handle(ctx))
                .isInstanceOfSatisfying(PermanentJobException.class, e -> {
                    assertThat(e.errorCode()).isEqualTo("INVALID_PAYLOAD");
                    assertThat(e.safeMessage()).doesNotContain("SECRET").doesNotContain("9876");
                    assertThat(e.getMessage()).doesNotContain("SECRET");
                    assertThat(e.getCause()).isNull();
                });
        assertThat(ctx.sends()).isEmpty();
    }

    @Test
    void longItemKeyRejected() {
        String id = "e".repeat(300);
        RecordingEffectContext ctx = ctx("{\"employeeIds\":[\"" + id + "\"],\"messageTemplate\":\"x\"}");
        assertThatThrownBy(() -> handler.handle(ctx))
                .isInstanceOfSatisfying(PermanentJobException.class,
                        e -> assertThat(e.errorCode()).isEqualTo("INVALID_PAYLOAD"));
    }
}
