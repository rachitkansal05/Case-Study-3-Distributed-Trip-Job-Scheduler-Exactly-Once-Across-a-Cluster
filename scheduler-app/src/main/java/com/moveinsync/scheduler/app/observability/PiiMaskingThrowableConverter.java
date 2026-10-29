package com.moveinsync.scheduler.app.observability;

import ch.qos.logback.classic.spi.ILoggingEvent;
import org.springframework.boot.logging.logback.ExtendedWhitespaceThrowableProxyConverter;

/**
 * Logback {@code %maskedEx}: Spring Boot's {@code %wEx} (stack trace with packaging data and
 * surrounding whitespace) with PII masked, since exception messages can carry PII.
 */
public final class PiiMaskingThrowableConverter extends ExtendedWhitespaceThrowableProxyConverter {

    @Override
    public String convert(ILoggingEvent event) {
        return PiiMasker.mask(super.convert(event));
    }
}
