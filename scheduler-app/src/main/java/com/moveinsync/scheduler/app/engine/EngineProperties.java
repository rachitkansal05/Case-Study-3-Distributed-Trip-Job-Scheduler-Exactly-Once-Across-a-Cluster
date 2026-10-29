package com.moveinsync.scheduler.app.engine;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * Engine-only knobs ({@code scheduler.engine.*}). The shared timing knobs (leases, intervals,
 * workers, retries) stay in {@link com.moveinsync.scheduler.app.config.SchedulerProperties}; this
 * record only adds what the Engine team introduced, so the Foundation-owned record is untouched.
 *
 * @param enabled when {@code false} no engine loop starts (API-only tests set this)
 * @param materializer run materialisation settings
 * @param compactor slot compaction settings
 * @param metrics DB-backed gauge refresh
 * @param clock DB clock offset refresh
 * @param epochGuard startup check against the sink's highest epoch (plan F5)
 * @param sink HTTP client timeouts
 */
@Validated
@ConfigurationProperties(prefix = "scheduler.engine")
public record EngineProperties(
        @DefaultValue("true") boolean enabled,
        @Valid @NotNull @DefaultValue Materializer materializer,
        @Valid @NotNull @DefaultValue Compactor compactor,
        @Valid @NotNull @DefaultValue Metrics metrics,
        @Valid @NotNull @DefaultValue Clock clock,
        @Valid @NotNull @DefaultValue EpochGuard epochGuard,
        @Valid @NotNull @DefaultValue SinkClient sink) {

    /**
     * @param interval how often the materialiser runs
     * @param horizon how far ahead runs are created (plan section 08: 15 minutes)
     * @param maxOccurrencesPerJob catch-up bound per job per pass; misfire policy handles the rest
     * @param jobBatchSize jobs examined per pass
     */
    public record Materializer(
            @NotNull @DefaultValue("5s") Duration interval,
            @NotNull @DefaultValue("15m") Duration horizon,
            @Min(1) @Max(1000) @DefaultValue("1000") int maxOccurrencesPerJob,
            @Min(1) @DefaultValue("500") int jobBatchSize) {
    }

    /** @param interval how often cap changes are applied (insert, delete, compaction moves) */
    public record Compactor(@NotNull @DefaultValue("1s") Duration interval) {
    }

    /** @param refreshInterval how often DB-count gauges are refreshed (plan section 14: 5 s) */
    public record Metrics(@NotNull @DefaultValue("5s") Duration refreshInterval) {
    }

    /** @param refreshInterval how often the DB clock offset is re-estimated */
    public record Clock(@NotNull @DefaultValue("30s") Duration refreshInterval) {
    }

    /**
     * @param enabled whether the node checks the sink epoch at startup
     * @param timeout how long to keep retrying an unreachable sink before refusing to start
     */
    public record EpochGuard(
            @DefaultValue("true") boolean enabled,
            @NotNull @DefaultValue("30s") Duration timeout) {
    }

    /**
     * @param connectTimeout TCP connect timeout
     * @param requestTimeout whole-request timeout; must stay below the self-fence budget
     */
    public record SinkClient(
            @NotNull @DefaultValue("2s") Duration connectTimeout,
            @NotNull @DefaultValue("5s") Duration requestTimeout) {
    }
}
