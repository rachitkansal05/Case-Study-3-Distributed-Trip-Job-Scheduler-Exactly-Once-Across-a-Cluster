package com.moveinsync.scheduler.app.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.app.error.ErrorCodes;
import com.moveinsync.scheduler.app.error.Problems;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.access.AccessDeniedHandler;

/**
 * 401 and 403 responses from the security filter chain in the same RFC 7807 shape as the
 * controller advice. The reason a token was rejected is never echoed.
 */
final class ProblemSecurityHandlers {

    private final ObjectMapper mapper;

    ProblemSecurityHandlers(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    AuthenticationEntryPoint entryPoint() {
        return (request, response, ex) -> {
            response.setHeader(HttpHeaders.WWW_AUTHENTICATE, "Bearer");
            write(response, Problems.of(HttpStatus.UNAUTHORIZED, ErrorCodes.UNAUTHORIZED,
                    "A valid bearer token is required"));
        };
    }

    AccessDeniedHandler accessDeniedHandler() {
        return (request, response, ex) -> write(response, Problems.of(HttpStatus.FORBIDDEN, ErrorCodes.FORBIDDEN,
                "Your role may not perform this action"));
    }

    private void write(HttpServletResponse response, ProblemDetail problem) throws IOException {
        response.setStatus(problem.getStatus());
        response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
        mapper.writeValue(response.getOutputStream(), problem);
    }
}
