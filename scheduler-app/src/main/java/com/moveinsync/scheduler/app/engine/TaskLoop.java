package com.moveinsync.scheduler.app.engine;

import java.time.Duration;
import java.util.Objects;

/** A {@link PeriodicLoop} around a plain task (materializer, gauges, clock refresh). */
final class TaskLoop extends PeriodicLoop {

    @FunctionalInterface
    interface Task {
        void run() throws Exception;
    }

    private final Task task;

    TaskLoop(String name, String nodeId, Duration interval, NodeGate gate, Task task) {
        super(name, nodeId, interval, gate);
        this.task = Objects.requireNonNull(task, "task");
    }

    @Override
    protected void tick() throws Exception {
        task.run();
    }
}
