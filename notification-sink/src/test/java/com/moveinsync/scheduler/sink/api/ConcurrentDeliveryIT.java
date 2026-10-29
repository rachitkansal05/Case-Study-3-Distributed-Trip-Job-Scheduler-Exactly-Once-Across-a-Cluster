package com.moveinsync.scheduler.sink.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.moveinsync.scheduler.core.domain.FencingToken;
import com.moveinsync.scheduler.sink.support.AbstractEmbeddedPostgresIT;
import com.moveinsync.scheduler.sink.support.SinkTestClient;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * 50 threads hammer the same 20 keys (spread over 4 runs) with mixed tokens. Proves, on real
 * Postgres: exactly one delivery per key; no request is accepted with a token below a fence value
 * that was already committed; and every attempt is accounted for.
 */
class ConcurrentDeliveryIT extends AbstractEmbeddedPostgresIT {

    private static final int THREADS = 50;
    private static final int REQUESTS_PER_THREAD = 40;
    private static final int RUNS = 4;
    private static final int KEYS_PER_RUN = 5;
    private static final FencingToken FINAL = new FencingToken(9, 0);

    @LocalServerPort
    private int port;

    @Autowired
    private JdbcClient jdbc;

    private record Sent(long runId, String key, FencingToken token, int status) {
    }

    private record Attempt(long runId, String key, FencingToken token, String outcome) {
    }

    @Test
    void exactlyOneDeliveryPerKeyAndFenceNeverGoesBackwards() throws Exception {
        SinkTestClient sink = new SinkTestClient(port);
        List<Long> runs = new ArrayList<>();
        Map<String, Long> runOfKey = new HashMap<>();
        for (int r = 0; r < RUNS; r++) {
            long run = SinkTestClient.newRunId();
            runs.add(run);
            for (int k = 0; k < KEYS_PER_RUN; k++) {
                runOfKey.put(SinkTestClient.newKey(run + ":emp-" + k), run);
            }
        }
        List<String> keys = List.copyOf(runOfKey.keySet());

        ConcurrentLinkedQueue<Sent> sent = new ConcurrentLinkedQueue<>();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < THREADS; t++) {
                long seed = 1_000L + t;
                futures.add(pool.submit(() -> {
                    Random random = new Random(seed);
                    start.await();
                    for (int i = 0; i < REQUESTS_PER_THREAD; i++) {
                        String key = keys.get(random.nextInt(keys.size()));
                        long run = runOfKey.get(key);
                        // epochs 7 and 8, seq 1..6: plenty of stale, equal and raising tokens
                        FencingToken token = new FencingToken(7 + random.nextInt(2), 1 + random.nextInt(6));
                        ResponseEntity<JsonNode> r = sink.deliver(run, key, token.toHeader());
                        sent.add(new Sent(run, key, token, r.getStatusCode().value()));
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get(2, TimeUnit.MINUTES);
            }
        } finally {
            pool.shutdownNow();
        }
        // Afterwards the highest token reaches every key, so every key ends up delivered.
        for (String key : keys) {
            int status = sink.deliver(runOfKey.get(key), key, FINAL.toHeader()).getStatusCode().value();
            assertThat(status).isIn(200, 201);
            sent.add(new Sent(runOfKey.get(key), key, FINAL, status));
        }

        int total = THREADS * REQUESTS_PER_THREAD + keys.size();
        assertThat(sent).hasSize(total);
        assertThat(sent).extracting(Sent::status).allMatch(s -> s == 200 || s == 201 || s == 409);

        // 1. Exactly one delivery per key: one row each, and exactly one 201 per key across all calls.
        Map<String, Long> rowsPerKey = jdbc.sql("SELECT idem_key, count(*) AS n FROM sink.delivery "
                        + "WHERE run_id IN (:runs) GROUP BY idem_key")
                .param("runs", runs)
                .query((rs, n) -> Map.entry(rs.getString("idem_key"), rs.getLong("n")))
                .list().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        assertThat(rowsPerKey).hasSize(keys.size()).containsOnlyKeys(keys).allSatisfy((k, n) -> assertThat(n).isOne());
        Map<String, Long> createdPerKey = sent.stream().filter(s -> s.status() == 201)
                .collect(Collectors.groupingBy(Sent::key, Collectors.counting()));
        assertThat(createdPerKey).hasSize(keys.size()).allSatisfy((k, n) -> assertThat(n).isOne());

        // 2. Attempt counts add up, outcome by outcome, with what clients were told.
        List<Attempt> attempts = jdbc.sql("SELECT run_id, idem_key, fencing_epoch, fencing_seq, outcome "
                        + "FROM sink.attempt_log WHERE run_id IN (:runs) ORDER BY id")
                .param("runs", runs)
                .query((rs, n) -> new Attempt(rs.getLong("run_id"), rs.getString("idem_key"),
                        new FencingToken(rs.getLong("fencing_epoch"), rs.getLong("fencing_seq")),
                        rs.getString("outcome")))
                .list();
        assertThat(attempts).hasSize(total);
        assertThat(count(attempts, "DELIVERED")).isEqualTo(statuses(sent, 201)).isEqualTo(keys.size());
        assertThat(count(attempts, "ALREADY_DELIVERED")).isEqualTo(statuses(sent, 200));
        assertThat(count(attempts, "STALE_TOKEN")).isEqualTo(statuses(sent, 409)).isPositive();

        // 3. Fence order. Attempt ids are drawn while the run's fence row is locked, so per run the
        //    id order is the commit order. Every accepted attempt must be >= every token accepted
        //    (committed) before it, and every rejection must be below that committed maximum.
        Map<Long, FencingToken> committedMax = new HashMap<>();
        Map<String, FencingToken> deliveredWith = new HashMap<>();
        for (Attempt a : attempts) {
            FencingToken max = committedMax.get(a.runId());
            if (a.outcome().equals("STALE_TOKEN")) {
                assertThat(max).as("a rejection needs an earlier accepted token").isNotNull();
                assertThat(a.token()).as("rejected token of run %d", a.runId()).isLessThan(max);
            } else {
                if (max != null) {
                    assertThat(a.token()).as("accepted token of run %d", a.runId()).isGreaterThanOrEqualTo(max);
                }
                committedMax.put(a.runId(), a.token());
                if (a.outcome().equals("DELIVERED")) {
                    deliveredWith.put(a.key(), a.token());
                }
            }
        }
        for (String key : keys) {
            FencingToken stored = jdbc.sql("SELECT fencing_epoch, fencing_seq FROM sink.delivery WHERE idem_key = :k")
                    .param("k", key)
                    .query((rs, n) -> new FencingToken(rs.getLong(1), rs.getLong(2)))
                    .single();
            assertThat(stored).isEqualTo(deliveredWith.get(key));
        }
        for (long run : runs) {
            FencingToken fence = jdbc.sql("SELECT max_epoch, max_seq FROM sink.run_fence WHERE run_id = :r")
                    .param("r", run)
                    .query((rs, n) -> new FencingToken(rs.getLong(1), rs.getLong(2)))
                    .single();
            assertThat(fence).isEqualTo(FINAL).isEqualTo(committedMax.get(run));

            // 4. The stats API agrees with the raw tables.
            JsonNode stats = sink.get("/api/v1/deliveries/stats?runId=" + run).getBody();
            List<Attempt> ofRun = attempts.stream().filter(a -> a.runId() == run).toList();
            assertThat(stats.get("deliveredDistinct").asLong()).isEqualTo(KEYS_PER_RUN);
            assertThat(stats.get("attempts").asLong()).isEqualTo(ofRun.size());
            assertThat(stats.get("alreadyDelivered").asLong()).isEqualTo(count(ofRun, "ALREADY_DELIVERED"));
            assertThat(stats.get("staleRejected").asLong()).isEqualTo(count(ofRun, "STALE_TOKEN"));
        }
        assertThat(sink.get("/api/v1/deliveries/stats").getBody().get("maxDeliveriesPerKey").asLong()).isOne();
    }

    private static long count(List<Attempt> attempts, String outcome) {
        return attempts.stream().filter(a -> a.outcome().equals(outcome)).count();
    }

    private static long statuses(ConcurrentLinkedQueue<Sent> sent, int status) {
        return sent.stream().filter(s -> s.status() == status).count();
    }
}
