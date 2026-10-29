package com.moveinsync.scheduler.app.service;

import java.time.Clock;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Fallback beans for the API layer. Each one backs off when another module supplies a real
 * implementation.
 */
@Configuration(proxyBeanMethods = false)
public class ServiceDefaultsConfig {

    /**
     * Local clock for API-side computations (token expiry, "fire times after now"). Due times
     * and leases are always decided by the database clock, never by this one.
     */
    @Bean
    @ConditionalOnMissingBean
    Clock clock() {
        return Clock.systemUTC();
    }
}
