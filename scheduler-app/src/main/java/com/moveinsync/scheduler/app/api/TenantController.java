package com.moveinsync.scheduler.app.api;

import com.moveinsync.scheduler.app.api.dto.CapRequest;
import com.moveinsync.scheduler.app.api.dto.CapView;
import com.moveinsync.scheduler.app.api.dto.CreateTenantRequest;
import com.moveinsync.scheduler.app.api.dto.TenantPauseView;
import com.moveinsync.scheduler.app.api.dto.TenantView;
import com.moveinsync.scheduler.app.security.Caller;
import com.moveinsync.scheduler.app.service.TenantService;
import jakarta.validation.Valid;
import java.net.URI;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** Tenant endpoints. Roles are enforced in {@code JwtSecurityConfig}; tenant scope in the service. */
@RestController
@RequestMapping("/api/v1/tenants")
public class TenantController {

    private final TenantService tenants;

    public TenantController(TenantService tenants) {
        this.tenants = tenants;
    }

    /** {@code POST /api/v1/tenants} (OPERATOR): 201, 409 {@code TENANT_EXISTS}. */
    @PostMapping
    public ResponseEntity<TenantView> create(Caller caller, @Valid @RequestBody CreateTenantRequest request) {
        TenantView view = tenants.create(caller, request);
        return ResponseEntity.created(URI.create("/api/v1/tenants/" + view.id())).body(view);
    }

    @PostMapping("/{tenantId}/pause")
    public TenantPauseView pause(Caller caller, @PathVariable String tenantId) {
        return tenants.setPaused(caller, tenantId, true);
    }

    @PostMapping("/{tenantId}/resume")
    public TenantPauseView resume(Caller caller, @PathVariable String tenantId) {
        return tenants.setPaused(caller, tenantId, false);
    }

    @PutMapping("/{tenantId}/cap")
    public CapView changeCap(Caller caller, @PathVariable String tenantId, @Valid @RequestBody CapRequest request) {
        return tenants.changeCap(caller, tenantId, request.cap());
    }
}
