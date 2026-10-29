package com.moveinsync.scheduler.app.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.slf4j.MDC;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Puts the authenticated caller's {@code tenantId} into the MDC for the rest of the request. Runs
 * inside the security chain right after bearer-token authentication (not registered as a bean,
 * so the servlet container does not run it a second time).
 */
final class TenantMdcFilter extends OncePerRequestFilter {

    static final String TENANT_MDC_KEY = "tenantId";

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth != null && auth.getPrincipal() instanceof Jwt jwt) {
            String tenant = jwt.getClaimAsString(JwtClaims.TENANT);
            if (tenant != null) {
                MDC.put(TENANT_MDC_KEY, tenant);
            }
        }
        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove(TENANT_MDC_KEY);
        }
    }
}
