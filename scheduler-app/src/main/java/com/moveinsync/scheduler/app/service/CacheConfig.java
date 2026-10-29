package com.moveinsync.scheduler.app.service;

import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Duration;
import java.util.List;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Caffeine caches with the bounds of PLAN.md section 14. Only these two named caches exist (no
 * dynamic creation), so nothing else can be cached by accident. <b>Caps and the pause flag are
 * never cached</b>: the cap is the slot rows and the claim reads {@code tenant.paused} directly.
 *
 * <p>Metrics: both caches are registered on the manager at startup and {@code recordStats()} is
 * on, so Spring Boot's cache metrics auto-configuration binds them to Micrometer
 * ({@code CaffeineCacheMeterBinderProvider}) as {@code cache_gets_total{cache,result}},
 * {@code cache_puts_total}, {@code cache_evictions_total} and {@code cache_size}, tagged
 * {@code cache="tenant-settings"} / {@code "next-fire-times"}. Keep {@code recordStats()}: without
 * it the hit and miss counters stay at 0. {@code MetricsIT} checks the export.
 */
@Configuration(proxyBeanMethods = false)
@EnableCaching
public class CacheConfig {

    /** Tenant settings (time zone only), keyed by tenant id: stale for at most 5 s. */
    public static final String TENANT_SETTINGS = "tenant-settings";
    public static final Duration TENANT_SETTINGS_TTL = Duration.ofSeconds(5);

    /** Next fire times keyed by (jobId, version, count): stale for at most 30 s after a zone change. */
    public static final String NEXT_FIRE_TIMES = "next-fire-times";
    public static final Duration NEXT_FIRE_TIMES_TTL = Duration.ofSeconds(30);

    @Bean
    CacheManager cacheManager() {
        CaffeineCacheManager manager = new CaffeineCacheManager();
        manager.registerCustomCache(TENANT_SETTINGS, Caffeine.newBuilder()
                .expireAfterWrite(TENANT_SETTINGS_TTL)
                .maximumSize(10_000)
                .recordStats()
                .build());
        manager.registerCustomCache(NEXT_FIRE_TIMES, Caffeine.newBuilder()
                .expireAfterWrite(NEXT_FIRE_TIMES_TTL)
                .maximumSize(50_000)
                .recordStats()
                .build());
        // only the caches above; an unknown cache name fails instead of silently caching
        manager.setCacheNames(List.of());
        return manager;
    }
}
