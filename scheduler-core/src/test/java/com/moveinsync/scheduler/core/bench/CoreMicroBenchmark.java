package com.moveinsync.scheduler.core.bench;

import com.moveinsync.scheduler.core.cluster.PartitionAssigner;
import com.moveinsync.scheduler.core.domain.TenantId;
import com.moveinsync.scheduler.core.fairness.DeficitRoundRobinDispatcher;
import com.moveinsync.scheduler.core.time.CronSchedule;
import com.moveinsync.scheduler.core.time.NextFireCalculator;
import com.moveinsync.scheduler.core.time.WallClockResolver;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.function.LongSupplier;

/**
 * JMH-free micro-benchmark that prints ns/op for the hot core algorithms. It is not a test (the
 * name does not end in {@code Test}), so Surefire never runs it. Run it by hand after {@code
 * test-compile}:
 *
 * <pre>{@code
 * java -cp scheduler-core/target/classes;scheduler-core/target/test-classes \
 *     com.moveinsync.scheduler.core.bench.CoreMicroBenchmark
 * }</pre>
 *
 * <p>Numbers are indicative only (no fork isolation, simple warm-up); use them to spot
 * regressions of an order of magnitude, not for fine comparisons.
 */
public final class CoreMicroBenchmark {

    private static final int WARMUP_ROUNDS = 3;
    private static final int MEASURED_ROUNDS = 5;

    private CoreMicroBenchmark() {
    }

    public static void main(String[] args) {
        ZoneId ny = ZoneId.of("America/New_York");
        LocalDateTime gap = LocalDateTime.of(2026, 3, 8, 2, 30);
        CronSchedule everyQuarter = CronSchedule.parse("*/15 * * * *");
        NextFireCalculator calculator = new NextFireCalculator();
        Instant start = Instant.parse("2026-10-31T00:00:00Z");
        PartitionAssigner assigner = new PartitionAssigner();
        List<String> nodes = List.of("scheduler-1", "scheduler-2", "scheduler-3");
        DeficitRoundRobinDispatcher<TenantId> drr = new DeficitRoundRobinDispatcher<>(t -> t, t -> 1);
        List<TenantId> tenants = List.of(TenantId.of("a"), TenantId.of("b"), TenantId.of("c"));

        run("WallClockResolver.resolve (gap)", 200_000, i -> WallClockResolver.resolve(gap, ny).getEpochSecond());
        run("NextFireCalculator.next(*/15, 10)", 20_000,
                i -> calculator.next(everyQuarter, ny, start, 10).size());
        run("PartitionAssigner.assign(128, 3)", 5_000, i -> assigner.assign(128, nodes).size());
        run("DRR offer + poll", 500_000, i -> {
            drr.offer(tenants.get((int) (i % 3)));
            return drr.poll().isPresent() ? 1 : 0;
        });
    }

    private interface Op {
        long apply(long i);
    }

    private static void run(String name, int iterations, Op op) {
        long sink = 0;
        double best = Double.MAX_VALUE;
        for (int round = 0; round < WARMUP_ROUNDS + MEASURED_ROUNDS; round++) {
            LongSupplier clock = System::nanoTime;
            long t0 = clock.getAsLong();
            for (long i = 0; i < iterations; i++) {
                sink += op.apply(i);
            }
            double nsPerOp = (double) (clock.getAsLong() - t0) / iterations;
            if (round >= WARMUP_ROUNDS) {
                best = Math.min(best, nsPerOp);
            }
        }
        System.out.printf("%-48s %12.1f ns/op  (sink %d)%n", name, best, sink);
    }
}
