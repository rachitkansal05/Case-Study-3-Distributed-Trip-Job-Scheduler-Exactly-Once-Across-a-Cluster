/**
 * PII-safe logging: {@link com.moveinsync.scheduler.app.observability.PiiMasker} masks emails,
 * coordinates and phone numbers; the logback converters and the JSON value masker apply it to
 * every log line (plain and JSON) as wired in {@code logback-spring.xml}.
 */
package com.moveinsync.scheduler.app.observability;
