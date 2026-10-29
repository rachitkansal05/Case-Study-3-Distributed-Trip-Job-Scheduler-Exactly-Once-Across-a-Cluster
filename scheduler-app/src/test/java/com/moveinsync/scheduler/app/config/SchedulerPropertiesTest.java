package com.moveinsync.scheduler.app.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.validation.ValidationAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;

/** Inconsistent timing knobs must stop the node at startup. No database needed. */
class SchedulerPropertiesTest {

    @Configuration
    @EnableConfigurationProperties(SchedulerProperties.class)
    static class Config {
    }

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(ValidationAutoConfiguration.class))
            .withUserConfiguration(Config.class)
            .withPropertyValues(
                    "scheduler.node-id=n1",
                    "scheduler.partitions=128",
                    "scheduler.run-lease.duration=10s",
                    "scheduler.run-lease.renew-interval=3s",
                    "scheduler.partition-lease.duration=6s",
                    "scheduler.partition-lease.renew-interval=2s",
                    "scheduler.partition-lease.loop-interval=1s",
                    "scheduler.membership.heartbeat-interval=2s",
                    "scheduler.membership.dead-after=6s",
                    "scheduler.claim.poll-interval=200ms",
                    "scheduler.orphan-sweep.interval=1s",
                    "scheduler.workers.count=64",
                    "scheduler.workers.reserved-for-recovery=1",
                    "scheduler.self-fence-margin=2s",
                    "scheduler.tenant.default-cap=32",
                    "scheduler.misfire.default-threshold=60s",
                    "scheduler.retry.max-attempts=5",
                    "scheduler.retry.lease-lost-limit=20",
                    "scheduler.retry.backoff-base=2s",
                    "scheduler.retry.backoff-cap=5m",
                    "scheduler.sink.base-url=http://sink:8090",
                    "scheduler.security.jwt-hmac-secret=0123456789abcdef0123456789abcdef");

    @Test
    void validConfigurationBinds() {
        runner.run(ctx -> assertThat(ctx).hasNotFailed().hasSingleBean(SchedulerProperties.class));
    }

    @Test
    void renewSlowerThanLeaseRejected() {
        runner.withPropertyValues("scheduler.run-lease.renew-interval=10s")
                .run(ctx -> assertThat(ctx).hasFailed());
    }

    @Test
    void selfFenceMarginLongerThanLeaseRejected() {
        runner.withPropertyValues("scheduler.self-fence-margin=11s")
                .run(ctx -> assertThat(ctx).hasFailed());
    }

    @Test
    void shortJwtSecretRejected() {
        runner.withPropertyValues("scheduler.security.jwt-hmac-secret=short")
                .run(ctx -> assertThat(ctx).hasFailed());
    }

    @Test
    void allWorkersReservedRejected() {
        runner.withPropertyValues("scheduler.workers.reserved-for-recovery=64")
                .run(ctx -> assertThat(ctx).hasFailed());
    }
}
