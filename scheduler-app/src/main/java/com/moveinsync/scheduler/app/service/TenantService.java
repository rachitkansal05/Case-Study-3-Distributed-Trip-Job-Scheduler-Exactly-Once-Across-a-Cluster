package com.moveinsync.scheduler.app.service;

import com.moveinsync.scheduler.app.api.dto.CapView;
import com.moveinsync.scheduler.app.api.dto.CreateTenantRequest;
import com.moveinsync.scheduler.app.api.dto.TenantPauseView;
import com.moveinsync.scheduler.app.api.dto.TenantView;
import com.moveinsync.scheduler.app.config.SchedulerProperties;
import com.moveinsync.scheduler.app.error.ConflictException;
import com.moveinsync.scheduler.app.error.ErrorCodes;
import com.moveinsync.scheduler.app.error.NotFoundException;
import com.moveinsync.scheduler.app.error.ValidationException;
import com.moveinsync.scheduler.app.repository.tenant.TenantRepository;
import com.moveinsync.scheduler.app.security.Caller;
import com.moveinsync.scheduler.app.security.TenantScope;
import java.time.ZoneId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Tenant administration: creation, pause/resume and cap changes (docs/design/architecture.md sections 10 and 13).
 *
 * <p>Pause and resume only flip {@code tenant.paused}. Runs stay {@code SCHEDULED}; the claim
 * reads the flag directly and the engine applies the misfire policy when it claims after a
 * resume. Caps are the slot rows themselves, so nothing here is cached.
 */
@Service
public class TenantService {

    private static final Logger log = LoggerFactory.getLogger(TenantService.class);

    private final TenantRepository tenants;
    private final SchedulerProperties properties;

    public TenantService(TenantRepository tenants, SchedulerProperties properties) {
        this.tenants = tenants;
        this.properties = properties;
    }

    /** Inserts the tenant and its {@code cap} slot rows in one transaction. */
    @Transactional
    public TenantView create(Caller caller, CreateTenantRequest request) {
        ZoneId zone = parseZone(request.timeZone());
        int cap = request.cap() == null ? properties.tenant().defaultCap() : request.cap();
        TenantScope scope = caller.scope().narrowTo(request.id());
        if (!tenants.insert(scope, request.id(), zone.getId(), cap)) {
            throw new ConflictException(ErrorCodes.TENANT_EXISTS, "Tenant already exists");
        }
        tenants.insertMissingSlots(scope, request.id(), cap);
        log.info("tenant created with cap {}", cap);
        return new TenantView(request.id(), zone.getId(), false, cap, cap, false);
    }

    @Transactional
    public TenantPauseView setPaused(Caller caller, String tenantId, boolean paused) {
        TenantScope scope = caller.scope().narrowTo(tenantId);
        if (!tenants.setPaused(scope, tenantId, paused)) {
            throw NotFoundException.tenant();
        }
        log.info("tenant {}", paused ? "paused" : "resumed");
        return new TenantPauseView(tenantId, paused);
    }

    /**
     * Changes the cap in one transaction. The tenant row is locked first, so concurrent cap
     * changes of a tenant run one after the other. Raising inserts the missing slot rows at once;
     * lowering deletes only free slot rows at or above the new cap, and slots in use drain through
     * the Engine's compactor ({@code draining = true} until then).
     */
    @Transactional
    public CapView changeCap(Caller caller, String tenantId, int cap) {
        TenantScope scope = caller.scope().narrowTo(tenantId);
        int previous = tenants.lockForCapChange(scope, tenantId).orElseThrow(NotFoundException::tenant);
        tenants.updateConfiguredCap(scope, tenantId, cap);
        int added = tenants.insertMissingSlots(scope, tenantId, cap);
        int removed = tenants.deleteFreeSlotsFrom(scope, tenantId, cap);
        int effective = tenants.countSlots(scope, tenantId);
        log.info("tenant cap changed {} -> {}: {} slots added, {} removed, effective {}",
                previous, cap, added, removed, effective);
        return CapView.of(tenantId, cap, effective);
    }

    /** Parses an IANA region id; fixed offsets and unknown ids are rejected. */
    static ZoneId parseZone(String timeZone) {
        if (timeZone == null || !ZoneId.getAvailableZoneIds().contains(timeZone)) {
            throw ValidationException.field("timeZone", "must be an IANA time zone id such as Asia/Kolkata");
        }
        return ZoneId.of(timeZone);
    }
}
