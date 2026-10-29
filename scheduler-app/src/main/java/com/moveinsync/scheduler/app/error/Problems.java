package com.moveinsync.scheduler.app.error;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;

/**
 * Builds the RFC 7807 bodies used by the advice and the security entry points, so every error has
 * the same shape: {@code type, title, status, detail, instance (urn:trace:<traceId>), code,
 * traceId} and, for validation errors,
 * {@code errors: [{field, reason}]}.
 */
public final class Problems {

    /** MDC key holding the current request's trace id (set by the request tracing filter). */
    public static final String TRACE_ID_MDC_KEY = "traceId";
    private static final Pattern SAFE_TRACE_ID = Pattern.compile("[A-Za-z0-9-]{1,64}");

    private Problems() {
    }

    public static ProblemDetail of(HttpStatus status, String code, String safeMessage) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, safeMessage);
        problem.setTitle(status.getReasonPhrase());
        problem.setProperty("code", code);
        String traceId = MDC.get(TRACE_ID_MDC_KEY);
        if (traceId != null) {
            problem.setProperty("traceId", traceId);
        }
        // Spring MVC fills a missing instance with the raw request path, which can carry a
        // rejected value (e.g. an email in a path variable). Use the trace id instead.
        problem.setInstance(URI.create(traceId != null && SAFE_TRACE_ID.matcher(traceId).matches()
                ? "urn:trace:" + traceId : "about:blank"));
        return problem;
    }

    public static ProblemDetail of(SchedulerException e) {
        ProblemDetail problem = of(e.status(), e.code(), e.getMessage());
        if (e instanceof ValidationException v && !v.fields().isEmpty()) {
            problem.setProperty("errors", fieldList(v.fields()));
        }
        return problem;
    }

    static List<Map<String, String>> fieldList(List<ValidationException.FieldIssue> issues) {
        return issues.stream()
                .map(i -> Map.of("field", i.field(), "reason", i.reason()))
                .toList();
    }
}
