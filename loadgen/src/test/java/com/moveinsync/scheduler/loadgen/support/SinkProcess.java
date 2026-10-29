package com.moveinsync.scheduler.loadgen.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The real {@code notification-sink} Spring Boot application, started from its executable jar in a
 * separate JVM on a free port, against its own database. Separate process on purpose: the sink and
 * the scheduler both ship {@code application.yml}, {@code logback-spring.xml} and a Flyway
 * {@code db/migration/V1__*} on their classpath, so they cannot share one JVM classpath without
 * one shadowing the other; in production they are separate processes too.
 *
 * <p>The jar path comes from the system property {@code sink.jar} (set by the loadgen Failsafe
 * configuration to {@code notification-sink/target/notification-sink-<version>.jar}).
 */
public final class SinkProcess implements AutoCloseable {

    public static final String ADMIN_TOKEN = "dev-only-sink-admin-token";

    private final Process process;
    private final URI base;
    private final Path log;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private final ObjectMapper mapper = new ObjectMapper();

    private SinkProcess(Process process, URI base, Path log) {
        this.process = process;
        this.base = base;
        this.log = log;
    }

    /**
     * Starts the sink against database {@code db} on {@code pg} (created fresh here) and waits until
     * {@code /actuator/health} is UP.
     *
     * @param poolSize sink Hikari pool size
     */
    public static SinkProcess start(EmbeddedPg pg, String db, int poolSize) {
        pg.createDatabase(db);
        String jar = System.getProperty("sink.jar");
        if (jar == null || !new File(jar).isFile()) {
            throw new IllegalStateException("notification-sink jar not found (system property sink.jar=" + jar
                    + "); build it with ./mvnw -pl loadgen -am verify");
        }
        int port = freePort();
        Path logFile = Path.of("target", "sink-" + db + ".log").toAbsolutePath();
        try {
            Files.createDirectories(logFile.getParent());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        List<String> cmd = new ArrayList<>();
        cmd.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        cmd.add("-Xmx768m");
        cmd.add("-XX:+UseG1GC");
        cmd.add("-jar");
        cmd.add(jar);
        cmd.add("--server.port=" + port);
        cmd.add("--spring.datasource.url=jdbc:postgresql://localhost:" + pg.port() + "/" + db);
        cmd.add("--spring.datasource.username=" + EmbeddedPg.USER);
        cmd.add("--spring.datasource.password=");
        cmd.add("--spring.datasource.hikari.maximum-pool-size=" + poolSize);
        cmd.add("--server.tomcat.threads.max=" + Math.max(50, poolSize * 4));
        cmd.add("--logging.level.root=WARN");
        cmd.add("--logging.level.org.springframework.boot.web=INFO");
        ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.to(logFile.toFile()));
        pb.environment().remove("SPRING_DATASOURCE_URL");
        pb.environment().remove("SERVER_PORT");
        Process p;
        try {
            p = pb.start();
        } catch (IOException e) {
            throw new UncheckedIOException("could not start the sink", e);
        }
        SinkProcess sink = new SinkProcess(p, URI.create("http://localhost:" + port), logFile);
        Runtime.getRuntime().addShutdownHook(new Thread(p::destroyForcibly, "sink-stop"));
        sink.awaitHealthy(Duration.ofSeconds(120));
        return sink;
    }

    private void awaitHealthy(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) {
                throw new IllegalStateException("sink exited with " + process.exitValue() + "; see " + log);
            }
            try {
                HttpResponse<String> r = http.send(HttpRequest.newBuilder(base.resolve("/actuator/health"))
                        .timeout(Duration.ofSeconds(2)).GET().build(), HttpResponse.BodyHandlers.ofString());
                if (r.statusCode() == 200 && r.body().contains("UP")) {
                    return;
                }
            } catch (IOException e) {
                // not listening yet
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        throw new IllegalStateException("sink not healthy within " + timeout + "; see " + log);
    }

    public URI baseUrl() {
        return base;
    }

    /** {@code GET /api/v1/deliveries/stats?runId=}. */
    public RunStats runStats(long runId) {
        JsonNode n = get("/api/v1/deliveries/stats?runId=" + runId);
        return new RunStats(n.path("deliveredDistinct").asLong(), n.path("attempts").asLong(),
                n.path("delivered").asLong(), n.path("alreadyDelivered").asLong(), n.path("staleRejected").asLong());
    }

    /** {@code GET /api/v1/deliveries/stats}. */
    public TotalStats totals() {
        JsonNode n = get("/api/v1/deliveries/stats");
        return new TotalStats(n.path("deliveredDistinct").asLong(), n.path("attempts").asLong(),
                n.path("delivered").asLong(), n.path("alreadyDelivered").asLong(),
                n.path("staleRejected").asLong(), n.path("runs").asLong(), n.path("maxDeliveriesPerKey").asLong());
    }

    /** {@code GET /api/v1/fence/highest-epoch}. */
    public long highestEpoch() {
        return get("/api/v1/fence/highest-epoch").path("highestEpoch").asLong();
    }

    /** {@code PUT /admin/faults} with latency only. */
    public void setLatency(int latencyMs, int jitterMs) {
        String body = "{\"latencyMs\":" + latencyMs + ",\"latencyJitterMs\":" + jitterMs + "}";
        HttpResponse<String> r = send(HttpRequest.newBuilder(base.resolve("/admin/faults"))
                .header("X-Admin-Token", ADMIN_TOKEN).header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(body)).build());
        if (r.statusCode() != 200) {
            throw new IllegalStateException("PUT /admin/faults -> " + r.statusCode());
        }
    }

    /**
     * {@code POST /api/v1/deliveries} directly (tests only, e.g. to seed a fence).
     *
     * @return the HTTP status
     */
    public int deliver(long runId, String idemKey, String token) {
        String body = "{\"runId\":" + runId + ",\"channel\":\"SMS\",\"payload\":{}}";
        return send(HttpRequest.newBuilder(base.resolve("/api/v1/deliveries"))
                .header("Content-Type", "application/json").header("Idempotency-Key", idemKey)
                .header("Fencing-Token", token).POST(HttpRequest.BodyPublishers.ofString(body)).build()).statusCode();
    }

    private JsonNode get(String path) {
        HttpResponse<String> r = send(HttpRequest.newBuilder(base.resolve(path)).GET().build());
        if (r.statusCode() != 200) {
            throw new IllegalStateException("GET " + path + " -> " + r.statusCode());
        }
        try {
            return mapper.readTree(r.body());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private HttpResponse<String> send(HttpRequest request) {
        try {
            return http.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() {
        process.destroy();
        try {
            if (!process.waitFor(15, TimeUnit.SECONDS)) {
                process.destroyForcibly();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }
    }

    private static int freePort() {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public record RunStats(long deliveredDistinct, long attempts, long delivered, long alreadyDelivered,
                           long staleRejected) {
    }

    public record TotalStats(long deliveredDistinct, long attempts, long delivered, long alreadyDelivered,
                             long staleRejected, long runs, long maxDeliveriesPerKey) {
    }
}
