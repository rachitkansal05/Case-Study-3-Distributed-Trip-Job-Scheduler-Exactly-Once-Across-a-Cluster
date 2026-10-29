package com.moveinsync.scheduler.sink.web;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Objects;
import org.springframework.web.servlet.HandlerInterceptor;

/**
 * Requires header {@value #HEADER} to equal the configured admin token (constant-time compare).
 * A missing or wrong token is a 401 {@code ADMIN_UNAUTHORIZED} ProblemDetail.
 */
public class AdminTokenInterceptor implements HandlerInterceptor {

    /** Header carrying the admin token. */
    public static final String HEADER = "X-Admin-Token";

    private final byte[] expected;

    public AdminTokenInterceptor(String token) {
        this.expected = Objects.requireNonNull(token, "token").getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        String presented = request.getHeader(HEADER);
        if (presented == null || !MessageDigest.isEqual(expected, presented.getBytes(StandardCharsets.UTF_8))) {
            throw new SinkProblemException(ProblemCode.ADMIN_UNAUTHORIZED,
                    "A valid " + HEADER + " header is required for admin routes.");
        }
        return true;
    }
}
