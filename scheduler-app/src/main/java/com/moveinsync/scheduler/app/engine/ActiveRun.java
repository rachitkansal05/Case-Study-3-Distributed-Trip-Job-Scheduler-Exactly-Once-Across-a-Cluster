package com.moveinsync.scheduler.app.engine;

import com.moveinsync.scheduler.core.domain.ClaimedRun;
import com.moveinsync.scheduler.core.domain.LeaseLostReason;
import com.moveinsync.scheduler.core.lease.LeaseClock;
import java.util.Objects;
import java.util.concurrent.Future;

/**
 * One run executing on this node: its claim, its self-fence ({@link LeaseClock}), and the worker
 * thread to interrupt when the lease is lost or the max run time passes. Thread-safe.
 *
 * <p>The attempt's outcome is written once: by the worker, or by the engine's hard stop when the
 * handler ignores the max-run-time interrupt ({@link #takeCommit()} decides who).
 */
final class ActiveRun {

    private final ClaimedRun run;
    private final Thread worker;
    private volatile LeaseClock leaseClock;
    private volatile LeaseLostReason lost;
    private volatile boolean timedOut;
    private volatile boolean hardStopped;
    private boolean finished;
    private boolean commitTaken;
    private Future<?> renewTask;
    private Future<?> timeoutTask;
    private Future<?> hardStopTask;

    ActiveRun(ClaimedRun run, LeaseClock leaseClock, Thread worker) {
        this.run = Objects.requireNonNull(run, "run");
        this.leaseClock = Objects.requireNonNull(leaseClock, "leaseClock");
        this.worker = Objects.requireNonNull(worker, "worker");
    }

    ClaimedRun run() {
        return run;
    }

    LeaseClock leaseClock() {
        return leaseClock;
    }

    /** Lease lost reason, or null while the lease is held. */
    LeaseLostReason lost() {
        return lost;
    }

    boolean timedOut() {
        return timedOut;
    }

    /** Self-fence: no lease loss recorded, not timed out, and the monotonic deadline is ahead. */
    boolean mayStartEffect() {
        return lost == null && !timedOut && leaseClock.mayStartEffect();
    }

    /** Called only after the DB confirmed a renewal; never moves the deadline backwards. */
    synchronized void renewed(long renewStartedNanos) {
        if (lost == null) {
            leaseClock = leaseClock.renewed(renewStartedNanos);
        }
    }

    /** Records the first lease loss and, unless the run already ended, interrupts the handler. */
    synchronized void loseLease(LeaseLostReason reason, boolean interrupt) {
        if (lost == null) {
            lost = reason;
        }
        if (interrupt && !finished) {
            worker.interrupt();
        }
    }

    synchronized void timeout() {
        if (!finished) {
            timedOut = true;
            worker.interrupt();
        }
    }

    /**
     * The right to write this attempt's outcome (commit or failure): true for exactly one caller,
     * the worker or the hard stop, so a late worker never writes over the hard stop's TIMEOUT.
     */
    synchronized boolean takeCommit() {
        if (commitTaken) {
            return false;
        }
        commitTaken = true;
        return true;
    }

    /** The engine recorded TIMEOUT for a handler that did not stop: no renewals, no interrupts. */
    synchronized void hardStop() {
        hardStopped = true;
        finish();
    }

    /** True once the hard stop wrote this attempt's outcome; the worker then writes nothing. */
    boolean hardStopped() {
        return hardStopped;
    }

    synchronized void tasks(Future<?> renew, Future<?> timeout, Future<?> hardStop) {
        this.renewTask = renew;
        this.timeoutTask = timeout;
        this.hardStopTask = hardStop;
    }

    /** Ends the run: no more interrupts, background tasks cancelled. */
    synchronized void finish() {
        finished = true;
        if (renewTask != null) {
            renewTask.cancel(false);
        }
        if (timeoutTask != null) {
            timeoutTask.cancel(false);
        }
        if (hardStopTask != null) {
            hardStopTask.cancel(false);
        }
    }

    synchronized boolean finished() {
        return finished;
    }
}
