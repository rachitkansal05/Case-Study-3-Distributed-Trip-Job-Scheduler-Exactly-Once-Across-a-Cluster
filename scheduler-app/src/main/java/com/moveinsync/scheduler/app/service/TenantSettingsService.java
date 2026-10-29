package com.moveinsync.scheduler.app.service;

import com.moveinsync.scheduler.app.error.NotFoundException;
import com.moveinsync.scheduler.app.repository.tenant.TenantRepository;
import com.moveinsync.scheduler.app.security.TenantScope;
import java.time.ZoneId;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

/**
 * Tenant settings (time zone) for job creation and next-fire-time queries. Callers pass a tenant
 * id they are already authorised for (the token's tenant, or the tenant of a job row read within
 * the caller's scope).
 */
@Service
public class TenantSettingsService {

    private final TenantRepository tenants;

    public TenantSettingsService(TenantRepository tenants) {
        this.tenants = tenants;
    }

    /**
     * Cached for {@link CacheConfig#TENANT_SETTINGS_TTL} (5 s). Only the time zone is cached;
     * the pause flag and cap are always read from the database.
     *
     * @throws NotFoundException when the tenant does not exist (not cached)
     */
    @Cacheable(cacheNames = CacheConfig.TENANT_SETTINGS, key = "#tenantId")
    public TenantSettings settings(String tenantId) {
        return tenants.find(TenantScope.of(tenantId), tenantId)
                .map(t -> new TenantSettings(t.id(), ZoneId.of(t.timeZone())))
                .orElseThrow(NotFoundException::tenant);
    }
}
