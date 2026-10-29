package com.moveinsync.scheduler.app.api;

import com.moveinsync.scheduler.app.api.dto.CapRequest;
import com.moveinsync.scheduler.app.api.dto.CreateTenantRequest;
import com.moveinsync.scheduler.app.api.dto.TenantView;
import com.moveinsync.scheduler.app.security.Caller;
import com.moveinsync.scheduler.app.service.IdempotencyService;
import com.moveinsync.scheduler.app.service.TenantService;
import jakarta.validation.Valid;
import java.net.URI;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Tenant endpoints. Roles are enforced in {@code JwtSecurityConfig}, the tenant scope in the
 * service. Pause, resume and cap accept an optional {@code Idempotency-Key}.
 */
@RestController
@RequestMapping("/api/v1/tenants")
public class TenantController {

    private static final String BASE = "/api/v1/tenants/";

    private final TenantService tenants;
    private final IdempotencyService idempotency;

    public TenantController(TenantService tenants, IdempotencyService idempotency) {
        this.tenants = tenants;
        this.idempotency = idempotency;
    }

    /** {@code POST /api/v1/tenants} (OPERATOR): 201; 409 {@code TENANT_EXISTS}. */
    @PostMapping
    public ResponseEntity<TenantView> create(Caller caller, @Valid @RequestBody CreateTenantRequest request) {
        TenantView view = tenants.create(caller, request);
        return ResponseEntity.created(URI.create(BASE + view.id())).body(view);
    }

    @PostMapping("/{tenantId}/pause")
    public ResponseEntity<?> pause(Caller caller, @PathVariable String tenantId,
                                   @RequestHeader(value = IdempotencyKeys.HEADER, required = false) String key) {
        return idempotency.execute(caller, IdempotencyKeys.optional(key), "POST " + BASE + tenantId + "/pause", null,
                () -> ResponseEntity.ok(tenants.setPaused(caller, tenantId, true)));
    }

    @PostMapping("/{tenantId}/resume")
    public ResponseEntity<?> resume(Caller caller, @PathVariable String tenantId,
                                    @RequestHeader(value = IdempotencyKeys.HEADER, required = false) String key) {
        return idempotency.execute(caller, IdempotencyKeys.optional(key), "POST " + BASE + tenantId + "/resume", null,
                () -> ResponseEntity.ok(tenants.setPaused(caller, tenantId, false)));
    }

    /** Returns {@code configuredCap}, {@code effectiveCap} and {@code draining}. */
    @PutMapping("/{tenantId}/cap")
    public ResponseEntity<?> changeCap(Caller caller, @PathVariable String tenantId,
                                       @Valid @RequestBody CapRequest request,
                                       @RequestHeader(value = IdempotencyKeys.HEADER, required = false) String key) {
        return idempotency.execute(caller, IdempotencyKeys.optional(key), "PUT " + BASE + tenantId + "/cap", request,
                () -> ResponseEntity.ok(tenants.changeCap(caller, tenantId, request.cap())));
    }
}
