package com.moveinsync.scheduler.app.observability;

import com.fasterxml.jackson.core.JsonStreamContext;
import net.logstash.logback.mask.ValueMasker;

/**
 * JSON value masker for {@code MaskingJsonGeneratorDecorator}: masks every string value written
 * by the Logstash encoder (message, MDC values, stack trace).
 */
public final class PiiValueMasker implements ValueMasker {

    /** Returns the masked string, or {@code null} when the value is not a string or is unchanged. */
    @Override
    public Object mask(JsonStreamContext context, Object value) {
        if (!(value instanceof CharSequence chars)) {
            return null;
        }
        String original = chars.toString();
        String masked = PiiMasker.mask(original);
        return masked.equals(original) ? null : masked;
    }
}
