package com.moveinsync.scheduler.app.engine;

import com.moveinsync.scheduler.app.engine.RunExecutor.Ticket;
import com.moveinsync.scheduler.core.domain.ClaimedRun;
import com.moveinsync.scheduler.core.fairness.DeficitRoundRobinDispatcher;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Worker pool fed by {@link DeficitRoundRobinDispatcher} (plan section 10): recovered runs first,
 * tenants in deficit round robin, and {@code reservedForRecovery} workers (default 1) kept free for
 * orphan takeovers so a takeover never queues behind new work. Workers are virtual threads, at most
 * {@code workers} running at once.
 *
 * <p>Runs only enter after a committed claim, and the poller claims no more than {@link
 * #freeForNormal()}, so the queue stays short: nothing holds a lease while waiting in it for long.
 */
final class Dispatcher {

    private final DeficitRoundRobinDispatcher<Ticket> drr;
    private final int workers;
    private final Consumer<Ticket> executor;
    private final ExecutorService pool;
    private volatile Consumer<ClaimedRun> onFinished = run -> { };
    private int busy;
    private boolean accepting = true;

    Dispatcher(String nodeId, int workers, int reservedForRecovery, Consumer<Ticket> executor) {
        this.workers = workers;
        this.executor = Objects.requireNonNull(executor, "executor");
        this.drr = new DeficitRoundRobinDispatcher<>(t -> t.run().tenantId(), t -> t.run().priority(),
                t -> t.run().recovered(), reservedForRecovery);
        this.pool = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name(nodeId + "-worker-", 0).factory());
    }

    /**
     * Called on the worker thread after each run finished executing (its slot, if the commit
     * succeeded, is already free). Used by the poller to refill the tenant's slot at once.
     */
    void onFinished(Consumer<ClaimedRun> listener) {
        this.onFinished = Objects.requireNonNull(listener, "listener");
    }

    /** Enqueues committed claims and starts as many as the pool allows. */
    void submit(List<ClaimedRun> runs, long claimNanos) {
        for (ClaimedRun run : runs) {
            drr.offer(new Ticket(run, claimNanos));
        }
        pump();
    }

    /** Workers a normal (non-recovered) claim may still use: total - reserved - busy - queued. */
    synchronized int freeForNormal() {
        return Math.max(0, workers - drr.reservedForRecovery() - busy - drr.size());
    }

    /** Workers an orphan takeover may still use (includes the reserved ones). */
    synchronized int freeForRecovery() {
        return Math.max(0, workers - busy - drr.size());
    }

    synchronized int busy() {
        return busy;
    }

    synchronized int queued() {
        return drr.size();
    }

    private void pump() {
        while (true) {
            Ticket next;
            synchronized (this) {
                if (!accepting) {
                    return;
                }
                next = drr.poll(busy, workers).orElse(null);
                if (next == null) {
                    return;
                }
                busy++;
            }
            Ticket ticket = next;
            try {
                pool.execute(() -> {
                    try {
                        executor.accept(ticket);
                    } finally {
                        boolean live;
                        synchronized (this) {
                            busy--;
                            live = accepting;
                            notifyAll();
                        }
                        pump();
                        if (live) {
                            notifyFinished(ticket.run());
                        }
                    }
                });
            } catch (RuntimeException rejected) {
                synchronized (this) {
                    busy--;
                }
                return;
            }
        }
    }

    private void notifyFinished(ClaimedRun run) {
        try {
            onFinished.accept(run);
        } catch (RuntimeException e) {
            // a listener failure must never take a worker down; the next full poll claims anyway
        }
    }

    /**
     * Graceful: run what is already claimed (queued and running) to completion, up to {@code
     * timeout}. Claimed runs are never abandoned on purpose; anything left when the timeout ends
     * simply loses its lease and is taken over by another node.
     */
    void drain(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        synchronized (this) {
            while ((busy > 0 || !drr.isEmpty()) && System.nanoTime() < deadline) {
                try {
                    wait(Math.max(1, (deadline - System.nanoTime()) / 1_000_000));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            accepting = false;
        }
        pool.shutdown();
        try {
            pool.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Abrupt: interrupt every worker and drop the queue (kill -9 simulation). */
    void kill() {
        synchronized (this) {
            accepting = false;
            drr.drain();
        }
        pool.shutdownNow();
    }
}
