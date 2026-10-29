package com.moveinsync.scheduler.core.job;

import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.domain.JobKind;
import com.moveinsync.scheduler.core.domain.MisfirePolicyType;
import com.moveinsync.scheduler.core.domain.TenantId;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * A job definition (row of table {@code job}). Sealed: a job is either a {@link OneOffJob} (fires
 * once at {@code runAt}) or a {@link RecurringJob} (fires on a cron expression in a zone), so
 * callers can switch over it exhaustively.
 *
 * <p>Validation rules shared by both variants (mirroring the {@code job} table CHECKs), enforced in
 * the record constructors. A missing value throws {@link NullPointerException}; any other
 * violation throws {@link IllegalArgumentException}. Messages name the field but never echo the
 * payload.
 *
 * <ul>
 *   <li>{@code id}, {@code tenantId}, {@code handler}, {@code misfirePolicy}, durations: non-null.
 *   <li>{@code payloadJson}: non-null, not blank, and after trimming starts with <code>{</code> or
 *       <code>[</code>. The JSON itself is not parsed here (core has no dependencies); the
 *       database {@code JSONB} column does the full check.
 *   <li>{@code priority}: {@value #MIN_PRIORITY}-{@value #MAX_PRIORITY}, 1 being most urgent.
 *   <li>{@code maxRunTime}: strictly positive.
 *   <li>{@code misfireThreshold}: zero or positive.
 *   <li>{@code maxAttempts}: 1-{@value #MAX_ATTEMPTS_LIMIT} (the column is a {@code SMALLINT}).
 *   <li>{@code version}: zero or positive; a new job starts at 0 (the column default) and every
 *       edit increments it.
 * </ul>
 */
public sealed interface JobDefinition permits OneOffJob, RecurringJob {

    /** Most urgent priority. */
    int MIN_PRIORITY = 1;

    /** Least urgent priority. */
    int MAX_PRIORITY = 9;

    /** Largest accepted {@code maxAttempts} ({@code SMALLINT} column). */
    int MAX_ATTEMPTS_LIMIT = Short.MAX_VALUE;

    /** Job id ({@code job.id}). */
    UUID id();

    /** Owning tenant. */
    TenantId tenantId();

    /** Business handler that runs the job. */
    HandlerType handler();

    /** Handler payload as JSON text; may hold PII, never log it. */
    String payloadJson();

    /** 1 (most urgent) to 9. */
    int priority();

    /** Longest a single attempt may run before it is timed out. */
    Duration maxRunTime();

    /** What happens to runs claimed later than {@link #misfireThreshold()}. */
    MisfirePolicyType misfirePolicy();

    /** Claim lateness above which a run counts as misfired. */
    Duration misfireThreshold();

    /** Attempts before a run goes to the dead-letter list. */
    int maxAttempts();

    /** Definition version (cache key), starting at 0. */
    long version();

    /** False while the job is paused. */
    boolean enabled();

    /** {@link JobKind#ONE_OFF} or {@link JobKind#RECURRING}, matching the concrete type. */
    JobKind kind();

    /**
     * The first fire instant strictly after {@code after}: for a one-off job its {@code runAt} if
     * that is after {@code after}, otherwise empty; for a recurring job the next cron occurrence
     * (empty if the expression can never fire). Does not look at {@link #enabled()}; callers decide
     * what a paused job means.
     */
    Optional<Instant> firstFireAfter(Instant after);
}
