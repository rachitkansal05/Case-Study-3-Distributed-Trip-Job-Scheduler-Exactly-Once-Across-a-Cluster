package com.moveinsync.scheduler.app.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.app.engine.support.FakeEffectSink;
import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.policy.PermanentJobException;
import java.util.List;
import org.junit.jupiter.api.Test;

class HandlerRegistryTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void defaultsCoverEveryHandlerType() {
        HandlerRegistry registry = HandlerRegistry.defaults(mapper);
        for (HandlerType type : HandlerType.values()) {
            assertThat(registry.get(type).type()).isEqualTo(type);
        }
        assertThat(registry.types()).containsExactlyInAnyOrder(HandlerType.values());
    }

    @Test
    void rejectsDuplicates() {
        assertThatThrownBy(() -> new HandlerRegistry(List.of(new BillingCloseHandler(mapper), new BillingCloseHandler(mapper))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void missingHandlerIsPermanent() {
        HandlerRegistry registry = new HandlerRegistry(List.of(new BillingCloseHandler(mapper)));
        assertThatThrownBy(() -> registry.get(HandlerType.ROSTER_GEN))
                .isInstanceOfSatisfying(PermanentJobException.class,
                        e -> assertThat(e.errorCode()).isEqualTo("NO_HANDLER"));
    }

    @Test
    void singleEffectHandlersUseRunKey() throws Exception {
        record Case(HandlerType type, String payload, String field) {}
        List<Case> cases = List.of(
                new Case(HandlerType.ROSTER_GEN, "{\"shiftDate\":\"2026-11-01\",\"siteId\":\"BLR-01\"}", "siteId"),
                new Case(HandlerType.NO_SHOW_CANCEL, "{\"tripId\":\"T1\",\"employeeId\":\"e42\"}", "tripId"),
                new Case(HandlerType.BILLING_CLOSE, "{\"period\":\"2026-10\",\"accountId\":\"ACME\"}", "accountId"));
        HandlerRegistry registry = HandlerRegistry.defaults(mapper);
        for (Case c : cases) {
            FakeEffectSink sink = new FakeEffectSink();
            RecordingEffectContext ctx = RecordingEffectContext.of(c.type(), c.payload(), null, sink);
            registry.get(c.type()).handle(ctx);
            assertThat(ctx.sends()).singleElement().satisfies(s -> {
                assertThat(s.idemKey()).isEqualTo("idem");
                JsonNode effect = mapper.readTree(s.payloadJson());
                assertThat(effect.isObject()).isTrue();
                assertThat(effect.hasNonNull(c.field())).isTrue();
            });
        }
    }

    @Test
    void singleEffectHandlersValidatePayload() {
        HandlerRegistry registry = HandlerRegistry.defaults(mapper);
        record Case(HandlerType type, String payload) {}
        for (Case c : List.of(
                new Case(HandlerType.ROSTER_GEN, "{\"shiftDate\":\"01/11/2026\",\"siteId\":\"S\"}"),
                new Case(HandlerType.ROSTER_GEN, "{\"shiftDate\":\"2026-11-01\"}"),
                new Case(HandlerType.NO_SHOW_CANCEL, "{\"tripId\":\"T1\"}"),
                new Case(HandlerType.NO_SHOW_CANCEL, "{\"tripId\":\"\",\"employeeId\":\"e\"}"),
                new Case(HandlerType.BILLING_CLOSE, "{\"period\":\"2026-13\",\"accountId\":\"A\"}"),
                new Case(HandlerType.BILLING_CLOSE, "{\"period\":\"2026-10\",\"accountId\":7}"))) {
            RecordingEffectContext ctx = RecordingEffectContext.of(c.type(), c.payload(), null, new FakeEffectSink());
            assertThatThrownBy(() -> registry.get(c.type()).handle(ctx))
                    .isInstanceOfSatisfying(PermanentJobException.class,
                            e -> assertThat(e.errorCode()).isEqualTo("INVALID_PAYLOAD"));
            assertThat(ctx.sends()).isEmpty();
        }
    }
}
