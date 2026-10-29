package com.moveinsync.scheduler.app.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.moveinsync.scheduler.app.engine.support.EngineTestDb;
import com.moveinsync.scheduler.core.time.CronSchedule;
import com.moveinsync.scheduler.core.time.NextFireCalculator;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * {@link Materializer} against a real migrated PostgreSQL. Every test uses its own partitions and
 * tenants, and each Materializer only owns the partitions of its test, so tests never see each
 * other's jobs. No sleeps: DST dates are reached by placing the cursor at the date and bounding the
 * pass with {@code maxOccurrencesPerJob}; "now"-relative cases read the DB clock around the pass.
 */
class MaterializerIT {

    private static final Duration TEN_YEARS = Duration.ofDays(3650);

    private static EngineTestDb db;
    private static JdbcClient jdbc;

    @BeforeAll
    static void setUp() {
        db = EngineTestDb.create("materializer_it");
        jdbc = db.jdbc();
    }

    @AfterAll
    static void tearDown() {
        if (db != null) {
            db.close();
        }
    }

    // ------------------------------------------------------------------ 1. DST

    @Test
    void dstGapAndOverlapInTenantZone() {
        tenant("t-ny", "America/New_York");
        UUID spring = recurring("t-ny", "30 2 * * *", 1, at("2026-03-07T07:30:00Z"), true);
        UUID fall = recurring("t-ny", "30 1 * * *", 1, at("2026-10-31T05:30:00Z"), true);

        Materializer.Result r = materializer(db.admin(), 1, TEN_YEARS, 3).materializeOnce();
        assertThat(r).isEqualTo(new Materializer.Result(2, 6, 2));

        // spring forward: 02:30 does not exist on 2026-03-08, fires at the gap end 03:00 EDT = 07:00Z
        assertThat(scheduled(spring)).containsExactly(
                at("2026-03-07T07:30:00Z"), at("2026-03-08T07:00:00Z"), at("2026-03-09T06:30:00Z"));
        assertThat(idemKeys(spring)).contains(spring + ":2026-03-08T07:00:00Z");
        assertThat(cursor(spring)).isEqualTo(at("2026-03-10T06:30:00Z"));

        // fall back: 01:30 occurs twice on 2026-11-01, fires once at the first (EDT) = 05:30Z
        assertThat(scheduled(fall)).containsExactly(
                at("2026-10-31T05:30:00Z"), at("2026-11-01T05:30:00Z"), at("2026-11-02T06:30:00Z"));
        assertThat(jdbc.sql("""
                SELECT count(*) FROM job_run WHERE job_id = ?
                   AND scheduled_at >= '2026-11-01T04:00:00Z' AND scheduled_at < '2026-11-02T05:00:00Z'""")
                .param(fall).query(Long.class).single()).isEqualTo(1L);
        assertThat(idemKeys(fall)).contains(fall + ":2026-11-01T05:30:00Z");
        assertThat(cursor(fall)).isEqualTo(at("2026-11-03T06:30:00Z"));
    }

    // ------------------------------------------------------------------ 2. three nodes

    @Test
    void idempotentAcrossThreeConcurrentNodes() throws Exception {
        tenant("t-n-utc", "UTC");
        tenant("t-n-ist", "Asia/Kolkata");
        // Hourly on Feb 29 only: 24 occurrences in 2024, next 2028, so the final state does not
        // depend on the current time (horizon 1 day) and the expected count is exact.
        String cron = "0 * 29 2 *";
        UUID a = recurring("t-n-utc", cron, 2, at("2024-02-29T00:00:00Z"), true);
        UUID b = recurring("t-n-utc", cron, 3, at("2024-02-29T00:00:00Z"), true);
        UUID c = recurring("t-n-ist", cron, 2, at("2024-02-28T18:30:00Z"), true); // 00:00 IST
        UUID d = recurring("t-n-ist", cron, 3, null, true);                       // init -> 2028
        UUID o1 = oneOff("t-n-utc", at("2026-01-01T00:00:00Z"), 2, 5, "5 minutes", 0);
        UUID o2 = oneOff("t-n-ist", at("2030-01-01T00:00:00Z"), 3, 5, "5 minutes", 0);

        int nodes = 3;
        int rounds = 12;
        List<Materializer> ms = IntStream.range(0, nodes)
                .mapToObj(i -> materializer(db.newPool("mat-node-" + i, 4), Set.of(2, 3),
                        Duration.ofDays(1), 5, 10))
                .toList();
        CyclicBarrier barrier = new CyclicBarrier(nodes);
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        ExecutorService pool = Executors.newFixedThreadPool(nodes);
        try {
            List<Future<Integer>> inserted = new ArrayList<>();
            for (Materializer m : ms) {
                inserted.add(pool.submit(() -> {
                    int sum = 0;
                    for (int i = 0; i < rounds; i++) {
                        barrier.await(30, TimeUnit.SECONDS);
                        try {
                            sum += m.materializeOnce().runsInserted();
                        } catch (Throwable t) {
                            errors.add(t);
                        }
                    }
                    return sum;
                }));
            }
            int total = 0;
            for (Future<Integer> f : inserted) {
                total += f.get(120, TimeUnit.SECONDS);
            }
            assertThat(errors).isEmpty();
            assertThat(total).isEqualTo(4 * 24 - 24 + 2); // d starts in 2028, beyond the horizon
        } finally {
            pool.shutdownNow();
        }

        for (UUID job : List.of(a, b, c)) {
            assertThat(scheduled(job)).hasSize(24).doesNotHaveDuplicates();
        }
        assertThat(scheduled(a)).first().isEqualTo(at("2024-02-29T00:00:00Z"));
        assertThat(scheduled(c)).last().isEqualTo(at("2024-02-29T17:30:00Z"));
        assertThat(cursor(a)).isEqualTo(at("2028-02-29T00:00:00Z"));
        assertThat(cursor(b)).isEqualTo(at("2028-02-29T00:00:00Z"));
        assertThat(cursor(c)).isEqualTo(at("2028-02-28T18:30:00Z"));
        assertThat(cursor(d)).isEqualTo(at("2028-02-28T18:30:00Z"));
        assertThat(scheduled(d)).isEmpty();
        assertThat(scheduled(o1)).containsExactly(at("2026-01-01T00:00:00Z"));
        assertThat(scheduled(o2)).containsExactly(at("2030-01-01T00:00:00Z"));
        assertThat(jdbc.sql("SELECT count(*) FROM job_run WHERE partition_id IN (2, 3)")
                .query(Long.class).single()).isEqualTo(3 * 24 + 2L);

        // stable: one more pass on any node changes nothing
        Materializer.Result again = ms.get(0).materializeOnce();
        assertThat(again.runsInserted()).isZero();
        assertThat(again.cursorsAdvanced()).isZero();
    }

    // ------------------------------------------------------------------ 3. bounded catch-up

    @Test
    void catchUpIsBoundedPerPass() {
        tenant("t-catchup", "UTC");
        UUID job = recurring("t-catchup", "0 * * * *", 4, null, true);
        // cursor = the top of the current UTC hour minus 3 hours (DB clock)
        jdbc.sql("""
                UPDATE job SET next_fire_at =
                    date_trunc('hour', clock_timestamp() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' - interval '3 hours'
                 WHERE id = ?""").param(job).update();
        Instant c0 = cursor(job);
        Materializer m = materializer(db.admin(), 4, Duration.ZERO, 2);

        assertThat(m.materializeOnce()).isEqualTo(new Materializer.Result(1, 2, 1));
        assertThat(scheduled(job)).containsExactly(c0, c0.plus(hours(1)));
        assertThat(cursor(job)).isEqualTo(c0.plus(hours(2)));

        assertThat(m.materializeOnce()).isEqualTo(new Materializer.Result(1, 2, 1));
        assertThat(scheduled(job)).containsExactly(c0, c0.plus(hours(1)), c0.plus(hours(2)), c0.plus(hours(3)));
        assertThat(cursor(job)).isEqualTo(c0.plus(hours(4)));

        Instant before = dbNow();
        m.materializeOnce();
        assertThat(cursor(job)).isAfter(before);

        assertThat(jdbc.sql("SELECT count(*) FROM job_run WHERE job_id = ? AND (state <> 'SCHEDULED' OR due_at <> scheduled_at)")
                .param(job).query(Long.class).single()).isZero();
    }

    // ------------------------------------------------------------------ 4. horizon

    @Test
    void horizonLimitsMaterialisation() {
        tenant("t-horizon", "UTC");
        UUID job = recurring("t-horizon", "*/5 * * * *", 5, null, true);
        Duration horizon = Duration.ofMinutes(15);
        Materializer m = materializer(db.admin(), 5, horizon, 100);

        Instant t0 = dbNow();
        Materializer.Result r = m.materializeOnce();
        Instant t1 = dbNow();

        List<Instant> runs = scheduled(job);
        // first occurrence f1 is in (now, now + 5m]; f1, f1+5m, f1+10m <= now + 15m < f1 + 15m
        assertThat(runs).hasSize(3);
        assertThat(r).isEqualTo(new Materializer.Result(1, 3, 1));
        assertThat(runs.get(0)).isAfter(t0);
        assertThat(runs.get(1)).isEqualTo(runs.get(0).plus(Duration.ofMinutes(5)));
        assertThat(runs.get(2)).isEqualTo(runs.get(0).plus(Duration.ofMinutes(10)));
        assertThat(runs.get(2)).isBeforeOrEqualTo(t1.plus(horizon));
        Instant next = cursor(job);
        assertThat(next).isEqualTo(runs.get(2).plus(Duration.ofMinutes(5)));
        assertThat(next).isAfter(t0.plus(horizon));
    }

    // ------------------------------------------------------------------ 5. one-off

    @Test
    void oneOffGetsExactlyOneRunAcrossNodesAndPasses() throws Exception {
        tenant("t-oneoff", "Asia/Kolkata");
        UUID job = UUID.randomUUID();
        jdbc.sql("""
                INSERT INTO job (id, tenant_id, kind, handler, run_at, payload, priority, max_run_time,
                                 misfire, partition_id, version, next_fire_at)
                VALUES (?, 't-oneoff', 'ONE_OFF', 'PICKUP_ALERT', clock_timestamp() + interval '7 days',
                        '{}'::jsonb, 2, interval '7 minutes', 'SKIP', 6, 3, clock_timestamp() + interval '7 days')""")
                .param(job).update();
        Instant runAt = jdbc.sql("SELECT run_at FROM job WHERE id = ?").param(job)
                .query(OffsetDateTime.class).single().toInstant();

        List<Materializer> ms = IntStream.range(0, 3)
                .mapToObj(i -> materializer(db.newPool("oneoff-node-" + i, 2), Set.of(6), Duration.ofMinutes(15), 10, 10))
                .toList();
        CyclicBarrier barrier = new CyclicBarrier(3);
        ExecutorService pool = Executors.newFixedThreadPool(3);
        try {
            List<Future<Integer>> fs = new ArrayList<>();
            for (Materializer m : ms) {
                fs.add(pool.submit(() -> {
                    int sum = 0;
                    for (int i = 0; i < 4; i++) {
                        barrier.await(30, TimeUnit.SECONDS);
                        sum += m.materializeOnce().runsInserted();
                    }
                    return sum;
                }));
            }
            int total = 0;
            for (Future<Integer> f : fs) {
                total += f.get(60, TimeUnit.SECONDS);
            }
            assertThat(total).isEqualTo(1);
        } finally {
            pool.shutdownNow();
        }
        assertThat(ms.get(1).materializeOnce()).isEqualTo(new Materializer.Result(0, 0, 0));

        var row = jdbc.sql("""
                SELECT partition_id, priority, job_version, max_run_time::text AS mrt, scheduled_at, due_at,
                       idem_key, state, tenant_id
                  FROM job_run WHERE job_id = ?""")
                .param(job).query().listOfRows();
        assertThat(row).hasSize(1);
        var run = row.get(0);
        assertThat(((Number) run.get("partition_id")).intValue()).isEqualTo(6);
        assertThat(((Number) run.get("priority")).intValue()).isEqualTo(2);
        assertThat(((Number) run.get("job_version")).longValue()).isEqualTo(3L);
        assertThat(run.get("mrt")).isEqualTo("00:07:00");
        assertThat(run.get("state")).isEqualTo("SCHEDULED");
        assertThat(run.get("tenant_id")).isEqualTo("t-oneoff");
        assertThat(scheduled(job)).containsExactly(runAt);
        assertThat(jdbc.sql("SELECT count(*) FROM job_run WHERE job_id = ? AND due_at = scheduled_at")
                .param(job).query(Long.class).single()).isEqualTo(1L);
        assertThat(run.get("idem_key")).isEqualTo(job + ":" + runAt);
        assertThat(jdbc.sql("SELECT next_fire_at IS NULL FROM job WHERE id = ?").param(job)
                .query(Boolean.class).single()).isTrue();
    }

    // ------------------------------------------------------------------ 6. ownership, enabled, NULL cursor

    @Test
    void ignoresUnownedAndDisabledAndInitialisesNullCursor() {
        tenant("t-own", "Europe/London");
        String cron = "0 0 1 1 *"; // yearly, so a zero horizon never reaches it
        UUID owned = recurring("t-own", cron, 7, null, true);
        UUID disabled = recurring("t-own", cron, 7, null, false);
        UUID disabledPast = recurring("t-own", "0 * * * *", 7, at("2025-01-01T00:00:00Z"), false);
        UUID unowned = recurring("t-own", "0 * * * *", 8, at("2025-01-01T00:00:00Z"), true);
        UUID unownedOneOff = oneOff("t-own", at("2025-01-01T00:00:00Z"), 8, 5, "1 minute", 0);
        UUID disabledOneOff = oneOff("t-own", at("2025-01-01T00:00:00Z"), 7, 5, "1 minute", 0);
        jdbc.sql("UPDATE job SET enabled = false WHERE id = ?").param(disabledOneOff).update();

        // empty ownership: nothing at all
        Materializer none = new Materializer(db.admin(), Set::of, Duration.ZERO, 10, 10);
        assertThat(none.materializeOnce()).isEqualTo(new Materializer.Result(0, 0, 0));
        assertThat(cursor(owned)).isNull();

        Instant t0 = dbNow();
        Materializer.Result r = materializer(db.admin(), 7, Duration.ZERO, 10).materializeOnce();
        Instant t1 = dbNow();
        assertThat(r).isEqualTo(new Materializer.Result(1, 0, 1));

        NextFireCalculator calc = new NextFireCalculator();
        CronSchedule schedule = CronSchedule.parse(cron);
        ZoneId london = ZoneId.of("Europe/London");
        Instant expected0 = calc.nextAfter(schedule, london, t0).orElseThrow().instant();
        Instant expected1 = calc.nextAfter(schedule, london, t1).orElseThrow().instant();
        assertThat(cursor(owned)).isIn(expected0, expected1).isAfter(t0);
        assertThat(scheduled(owned)).isEmpty();

        assertThat(cursor(disabled)).isNull();
        assertThat(cursor(disabledPast)).isEqualTo(at("2025-01-01T00:00:00Z"));
        assertThat(cursor(unowned)).isEqualTo(at("2025-01-01T00:00:00Z"));
        for (UUID j : List.of(disabled, disabledPast, unowned, unownedOneOff, disabledOneOff)) {
            assertThat(scheduled(j)).as("runs of %s", j).isEmpty();
        }
    }

    // ------------------------------------------------------------------ helpers

    private static Materializer materializer(DataSource ds, int partition, Duration horizon, int maxOcc) {
        return materializer(ds, Set.of(partition), horizon, maxOcc, 50);
    }

    private static Materializer materializer(DataSource ds, Set<Integer> partitions, Duration horizon,
                                             int maxOcc, int batch) {
        Supplier<Set<Integer>> owned = () -> partitions;
        return new Materializer(ds, owned, horizon, maxOcc, batch);
    }

    private static void tenant(String id, String zone) {
        jdbc.sql("INSERT INTO tenant (id, time_zone) VALUES (?, ?)").params(id, zone).update();
    }

    private static UUID recurring(String tenant, String cron, int partition, Instant cursor, boolean enabled) {
        UUID id = UUID.randomUUID();
        jdbc.sql("""
                INSERT INTO job (id, tenant_id, kind, handler, cron, payload, max_run_time, misfire,
                                 partition_id, next_fire_at, enabled)
                VALUES (?, ?, 'RECURRING', 'ROSTER_GEN', ?, '{"pii":"x"}'::jsonb, interval '5 minutes',
                        'FIRE_NOW', ?, ?, ?)""")
                .params(id, tenant, cron, partition, cursor == null ? null : cursor.atOffset(ZoneOffset.UTC), enabled)
                .update();
        return id;
    }

    private static UUID oneOff(String tenant, Instant runAt, int partition, int priority, String maxRunTime,
                               long version) {
        UUID id = UUID.randomUUID();
        jdbc.sql("""
                INSERT INTO job (id, tenant_id, kind, handler, run_at, payload, priority, max_run_time, misfire,
                                 partition_id, version, next_fire_at)
                VALUES (?, ?, 'ONE_OFF', 'PICKUP_ALERT', ?, '{}'::jsonb, ?, ?::interval, 'FIRE_NOW', ?, ?, ?)""")
                .params(id, tenant, runAt.atOffset(ZoneOffset.UTC), priority, maxRunTime, partition, version,
                        runAt.atOffset(ZoneOffset.UTC))
                .update();
        return id;
    }

    private static List<Instant> scheduled(UUID job) {
        return jdbc.sql("SELECT scheduled_at FROM job_run WHERE job_id = ? ORDER BY scheduled_at")
                .param(job).query(OffsetDateTime.class).list().stream()
                .map(OffsetDateTime::toInstant).collect(Collectors.toList());
    }

    private static List<String> idemKeys(UUID job) {
        return jdbc.sql("SELECT idem_key FROM job_run WHERE job_id = ? ORDER BY scheduled_at")
                .param(job).query(String.class).list();
    }

    private static Instant cursor(UUID job) {
        OffsetDateTime t = jdbc.sql("SELECT next_fire_at FROM job WHERE id = ?").param(job)
                .query(OffsetDateTime.class).list().get(0);
        return t == null ? null : t.toInstant();
    }

    private static Instant dbNow() {
        return jdbc.sql("SELECT clock_timestamp()").query(OffsetDateTime.class).single().toInstant();
    }

    private static Instant at(String s) {
        return Instant.parse(s);
    }

    private static Duration hours(int h) {
        return Duration.ofHours(h);
    }
}
