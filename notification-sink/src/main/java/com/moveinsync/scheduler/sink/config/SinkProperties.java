package com.moveinsync.scheduler.sink.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Sink settings under {@code sink.*}.
 *
 * @param admin admin route settings
 */
@ConfigurationProperties("sink")
public record SinkProperties(Admin admin) {

    public SinkProperties {
        if (admin == null) {
            throw new IllegalArgumentException("sink.admin.token must be set");
        }
    }

    /**
     * @param token static value required in the {@code X-Admin-Token} header on {@code /admin/**};
     *     env {@code SINK_ADMIN_TOKEN}. The dev default is deliberately obvious; override it outside dev.
     */
    public record Admin(String token) {

        public Admin {
            if (token == null || token.isBlank()) {
                throw new IllegalArgumentException("sink.admin.token must not be blank");
            }
        }

        /** Never prints the token. */
        @Override
        public String toString() {
            return "Admin[token=<redacted>]";
        }
    }
}
