package com.moveinsync.scheduler.app.engine;

import java.time.Duration;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * A loop on its own platform thread: {@code tick()}, then sleep {@link #nextDelay()}. A failing
 * tick is logged (never with payloads) and the loop continues; the DB being down must not kill the
 * node. Ticks are skipped while the node is frozen or dead.
 */
abstract class PeriodicLoop {

    private static final Logger log = LoggerFactory.getLogger(PeriodicLoop.class);

    private final String name;
    private final String nodeId;
    private final Duration interval;
    private final NodeGate gate;
    private volatile Thread thread;
    private volatile boolean running;
    private volatile long lastCompletedNanos = System.nanoTime();
    private int consecutiveFailures;

    PeriodicLoop(String name, String nodeId, Duration interval, NodeGate gate) {
        this.name = Objects.requireNonNull(name, "name");
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.interval = Objects.requireNonNull(interval, "interval");
        this.gate = Objects.requireNonNull(gate, "gate");
    }

    /** One iteration. */
    protected abstract void tick() throws Exception;

    /** Delay until the next tick; defaults to the configured interval. */
    protected Duration nextDelay() {
        return interval;
    }

    protected final Duration interval() {
        return interval;
    }

    /** Monotonic time of the last tick that completed without an exception. */
    final long lastCompletedNanos() {
        return lastCompletedNanos;
    }

    final synchronized void start() {
        if (thread != null) {
            return;
        }
        running = true;
        lastCompletedNanos = System.nanoTime();
        Thread t = Thread.ofPlatform().daemon().name(nodeId + "-" + name).unstarted(this::loop);
        thread = t;
        t.start();
    }

    /** Graceful: let the current tick finish, then stop. */
    final void stop(Duration timeout) {
        Thread t = thread;
        running = false;
        if (t == null) {
            return;
        }
        t.interrupt();
        try {
            t.join(timeout.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Abrupt: interrupt and walk away (kill -9 simulation). */
    final void kill() {
        running = false;
        Thread t = thread;
        if (t != null) {
            t.interrupt();
        }
    }

    private void loop() {
        MDC.put("nodeId", nodeId);
        while (running && !gate.isKilled()) {
            if (gate.isRunning()) {
                try {
                    tick();
                    lastCompletedNanos = System.nanoTime();
                    if (consecutiveFailures > 0) {
                        log.info("{} recovered after {} failed ticks", name, consecutiveFailures);
                    }
                    consecutiveFailures = 0;
                } catch (InterruptedException e) {
                    if (!running) {
                        break;
                    }
                    Thread.interrupted();
                } catch (NodeGate.NodeKilledException e) {
                    break;
                } catch (Exception e) {
                    if (!running || gate.isKilled()) {
                        break;
                    }
                    consecutiveFailures++;
                    if (consecutiveFailures == 1 || consecutiveFailures % 50 == 0) {
                        log.warn("{} tick failed ({} in a row): {}", name, consecutiveFailures, e.toString());
                    }
                }
            }
            try {
                Duration d = gate.isRunning() ? nextDelay() : Duration.ofMillis(20);
                Thread.sleep(Math.max(1, d.toMillis()));
            } catch (InterruptedException e) {
                if (!running) {
                    break;
                }
            }
        }
        MDC.clear();
    }
}
