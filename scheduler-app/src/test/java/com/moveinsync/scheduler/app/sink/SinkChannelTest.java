package com.moveinsync.scheduler.app.sink;

import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.scheduler.core.domain.HandlerType;
import org.junit.jupiter.api.Test;

class SinkChannelTest {

    @Test
    void mapsEveryHandler() {
        assertThat(SinkChannel.forHandler(HandlerType.PICKUP_ALERT)).isEqualTo(SinkChannel.SMS);
        assertThat(SinkChannel.forHandler(HandlerType.NO_SHOW_CANCEL)).isEqualTo(SinkChannel.SMS);
        assertThat(SinkChannel.forHandler(HandlerType.ROSTER_GEN)).isEqualTo(SinkChannel.PUSH);
        assertThat(SinkChannel.forHandler(HandlerType.BILLING_CLOSE)).isEqualTo(SinkChannel.PUSH);
    }

    @Test
    void namesMatchTheSinkApi() {
        assertThat(SinkChannel.values()).extracting(Enum::name).containsExactly("SMS", "PUSH");
    }
}
