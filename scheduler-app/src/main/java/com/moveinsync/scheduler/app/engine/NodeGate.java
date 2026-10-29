package com.moveinsync.scheduler.app.engine;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Process-level state of one node: running, frozen or dead. In production it is always running;
 * tests use {@link #freeze()} to simulate a stop-the-world pause (SIGSTOP / long GC) and {@link
 * #kill()} to simulate {@code kill -9} for nodes that share one JVM.
 *
 * <p>Every loop checks {@link #isRunning()} before a tick, and every effect and commit passes
 * {@link #pass()}. In particular a freeze can land <em>between</em> the self-fence check and the
 * send, which is exactly the window that sink fencing exists for (plan section 07, layer 2).
 */
public final class NodeGate {

    /** Thrown on a dead node's threads: the process is gone, nothing more may happen. */
    public static final class NodeKilledException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        NodeKilledException() {
            super("node killed", null, false, false);
        }
    }

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition thawed = lock.newCondition();
    private volatile boolean frozen;
    private volatile boolean killed;

    /** Blocks while frozen; throws once killed. */
    public void pass() throws InterruptedException {
        if (killed) {
            throw new NodeKilledException();
        }
        if (!frozen) {
            return;
        }
        lock.lock();
        try {
            while (frozen && !killed) {
                thawed.await();
            }
        } finally {
            lock.unlock();
        }
        if (killed) {
            throw new NodeKilledException();
        }
    }

    /** Like {@link #pass()} for code that cannot throw {@link InterruptedException}. */
    public void passUninterruptibly() {
        try {
            pass();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** True when neither frozen nor killed. */
    public boolean isRunning() {
        return !frozen && !killed;
    }

    public boolean isKilled() {
        return killed;
    }

    public boolean isFrozen() {
        return frozen;
    }

    public void freeze() {
        frozen = true;
    }

    public void unfreeze() {
        lock.lock();
        try {
            frozen = false;
            thawed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void kill() {
        lock.lock();
        try {
            killed = true;
            thawed.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
