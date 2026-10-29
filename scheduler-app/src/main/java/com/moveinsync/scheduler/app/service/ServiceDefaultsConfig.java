package com.moveinsync.scheduler.app.service;

import com.moveinsync.scheduler.app.config.SchedulerProperties;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Fallback beans for the API layer. Each one backs off when another module supplies a real
 * implementation (the merge adds adapters over Core's {@code NextFireCalculator} and
 * {@code PartitionHasher}).
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

    /** Placeholder until Core's murmur3 hasher is wired in: {@code floorMod(hashCode, partitions)}. */
    @Bean
    @ConditionalOnMissingBean
    PartitionPlacement partitionPlacement(SchedulerProperties properties) {
        int partitions = properties.partitions();
        return jobId -> Math.floorMod(jobId.hashCode(), partitions);
    }

    /** Lets the context start without Core's cron engine; any use fails with 500. */
    @Bean
    @ConditionalOnMissingBean
    FireTimeService unsupportedFireTimeService() {
        return new FireTimeService() {
            @Override
            public void validateCron(String cron) {
                throw new UnsupportedOperationException("no FireTimeService implementation is wired in");
            }

            @Override
            public List<FireTimeView> nextFireTimes(String cron, ZoneId zone, Instant after, int count) {
                throw new UnsupportedOperationException("no FireTimeService implementation is wired in");
            }
        };
    }
}
