package com.moveinsync.scheduler.app.engine;

import com.moveinsync.scheduler.core.domain.ClaimedRun;
import com.moveinsync.scheduler.core.domain.LeaseLostReason;
import com.moveinsync.scheduler.core.lease.LeaseClock;
import java.util.Objects;
import java.util.concurrent.Future;

/**
 * One run executing on this node: its claim, its self-fence ({@link LeaseClock}), and the worker
 * thread to interrupt when the lease is lost or the max run time passes. Thread-safe.
 */
final class ActiveRun {

    private final ClaimedRun run;
    private final Thread worker;
    private volatile LeaseClock leaseClock;
    private volatile LeaseLostReason lost;
    private volatile boolean timedOut;
    private boolean finished;
    private Future<?> renewTask;
    private Future<?> timeoutTask;

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

    synchronized void tasks(Future<?> renew, Future<?> timeout) {
        this.renewTask = renew;
        this.timeoutTask = timeout;
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
    }

    synchronized boolean finished() {
        return finished;
    }
}
