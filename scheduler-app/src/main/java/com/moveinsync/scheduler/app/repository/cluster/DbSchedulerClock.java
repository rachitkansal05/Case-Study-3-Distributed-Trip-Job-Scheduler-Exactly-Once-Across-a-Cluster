package com.moveinsync.scheduler.app.repository.cluster;

import com.moveinsync.scheduler.core.ports.SchedulerClock;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Objects;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * {@link SchedulerClock} on the database clock (plan section 02).
 *
 * <p>{@link #now()} is node time plus an estimated offset to {@code clock_timestamp()} on the
 * primary, re-estimated by {@link #refresh()} (the engine calls it periodically). The estimate is
 * used only for planning (which partitions look expired, misfire bookkeeping in logs); every
 * correctness decision is a SQL predicate on {@code clock_timestamp()} itself, so an offset error
 * can only add lag, never break fencing. {@link #monotonicNanos()} is {@link System#nanoTime()}.
 */
public final class DbSchedulerClock implements SchedulerClock {

    private static final Logger log = LoggerFactory.getLogger(DbSchedulerClock.class);

    private final JdbcClient jdbc;
    private final Clock local;
    private volatile long offsetNanos;

    public DbSchedulerClock(DataSource dataSource) {
        this(dataSource, Clock.systemUTC());
    }

    DbSchedulerClock(DataSource dataSource, Clock local) {
        this.jdbc = JdbcClient.create(Objects.requireNonNull(dataSource, "dataSource"));
        this.local = Objects.requireNonNull(local, "local");
    }

    /**
     * Re-estimates the offset as {@code db - midpoint(before, after)} of one round trip.
     *
     * @return the new offset
     */
    public Duration refresh() {
        Instant before = local.instant();
        long t0 = System.nanoTime();
        OffsetDateTime db = jdbc.sql("SELECT clock_timestamp()").query(OffsetDateTime.class).single();
        long rtt = System.nanoTime() - t0;
        Instant mid = before.plusNanos(rtt / 2);
        long offset = Duration.between(mid, db.toInstant()).toNanos();
        offsetNanos = offset;
        if (Math.abs(offset) > Duration.ofSeconds(1).toNanos()) {
            log.warn("node clock differs from the DB clock by {} ms; decisions still use the DB clock",
                    offset / 1_000_000);
        }
        return Duration.ofNanos(offset);
    }

    /** Current offset estimate (DB minus node). */
    public Duration offset() {
        return Duration.ofNanos(offsetNanos);
    }

    @Override
    public Instant now() {
        return local.instant().plusNanos(offsetNanos);
    }

    @Override
    public long monotonicNanos() {
        return System.nanoTime();
    }
}
