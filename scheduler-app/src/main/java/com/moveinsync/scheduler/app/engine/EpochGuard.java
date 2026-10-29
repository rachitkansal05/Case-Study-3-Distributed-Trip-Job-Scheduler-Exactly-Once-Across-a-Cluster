package com.moveinsync.scheduler.app.engine;

import com.moveinsync.scheduler.app.repository.claim.FencingEpochReader;
import com.moveinsync.scheduler.core.ports.EffectSink;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Startup check of plan F5: the node's current fencing epoch must not be lower than the highest
 * epoch the sink has ever accepted. A lower epoch means the database was rebuilt (fresh initdb,
 * timeline back at 1) or a stale primary is in use; every token we minted would be rejected, or
 * worse, look older than tokens already used. The node refuses to start; the operator raises
 * {@code cluster_meta.cluster_generation}.
 *
 * <p>Fails closed: if the sink cannot be read within {@code timeout} (non-200, malformed body, I/O
 * error) the node does not start either. It never assumes 0.
 */
public final class EpochGuard {

    private static final Logger log = LoggerFactory.getLogger(EpochGuard.class);

    /** The node must not start. */
    public static final class EpochGuardException extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        EpochGuardException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private final DataSource dataSource;
    private final EffectSink sink;
    private final Duration timeout;
    private final FencingEpochReader reader = new FencingEpochReader();

    public EpochGuard(DataSource dataSource, EffectSink sink, Duration timeout) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.sink = Objects.requireNonNull(sink, "sink");
        this.timeout = Objects.requireNonNull(timeout, "timeout");
    }

    /**
     * @return the current epoch, which is at least the sink's highest accepted epoch
     * @throws EpochGuardException if the epoch is lower, or either side cannot be read
     */
    public long check() {
        long current;
        try (Connection c = dataSource.getConnection()) {
            current = reader.read(c);
        } catch (SQLException | RuntimeException e) {
            throw new EpochGuardException("cannot read the current fencing epoch", e);
        }
        long highest = sinkHighest();
        if (current < highest) {
            throw new EpochGuardException("fencing epoch " + current + " is lower than the sink's highest accepted epoch "
                    + highest + "; raise cluster_meta.cluster_generation before starting (plan F5)", null);
        }
        log.info("epoch guard passed: current epoch {} >= sink highest {}", current, highest);
        return current;
    }

    private long sinkHighest() {
        long deadline = System.nanoTime() + timeout.toNanos();
        long backoffMs = 200;
        while (true) {
            try {
                return sink.highestAcceptedEpoch();
            } catch (RuntimeException e) {
                if (System.nanoTime() - deadline >= 0) {
                    throw new EpochGuardException("sink highest epoch unavailable; refusing to start", e);
                }
                log.warn("sink highest epoch unavailable, retrying: {}", e.toString());
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new EpochGuardException("interrupted while checking the sink epoch", ie);
                }
                backoffMs = Math.min(backoffMs * 2, 2_000);
            }
        }
    }
}
