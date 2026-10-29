package com.moveinsync.scheduler.loadgen;

import static com.moveinsync.scheduler.loadgen.MarkdownTable.f1;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Compose-mode load generator (plan section 17): drives a <b>running</b> stack over its public
 * HTTP API and reads the fire lag back from the database.
 *
 * <ol>
 *   <li>Mints tokens with {@code POST /dev/token} (dev profile). If that endpoint is absent and an
 *       HS256 secret is known ({@code --hmac-secret} or env {@code SCHEDULER_JWT_HMAC_SECRET}),
 *       mints the same tokens locally.</li>
 *   <li>Creates tenants ({@code POST /api/v1/tenants}, OPERATOR) and one-off {@code PICKUP_ALERT}
 *       jobs ({@code POST /api/v1/jobs}, tenant ADMIN, one {@code Idempotency-Key} per job, retried
 *       with the same key on 5xx or I/O errors), spread round robin over the given nodes with a
 *       bounded number of requests in flight (virtual threads + semaphore).</li>
 *   <li>Waits until every run of the created tenants is terminal, then reports lag
 *       ({@code started_at - scheduled_at} of {@code job_run}) with {@link RunTables}.</li>
 * </ol>
 *
 * <p>{@code runAt} comes from this machine's clock while {@code started_at} is the DB clock, so any
 * skew between the two shows up in the lag; on one laptop with Docker it is normally a few ms.
 */
public final class LoadGenMain {

    private static final DateTimeFormatter STAMP = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")
            .withZone(ZoneOffset.UTC);
    private static final String TEMPLATE = "Your cab is assigned, {employeeId}";

    private final LoadGenArgs args;
    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5)).build();
    private final StringBuilder report = new StringBuilder();
    private final Map<String, String> adminTokens = new HashMap<>();
    private boolean localTokens;

    LoadGenMain(LoadGenArgs args) {
        this.args = args;
    }

    public static void main(String[] argv) throws Exception {
        if (Arrays.asList(argv).contains("--help") || Arrays.asList(argv).contains("-h")) {
            System.out.print(LoadGenArgs.USAGE);
            return;
        }
        LoadGenArgs args;
        try {
            args = LoadGenArgs.parse(argv, System.getenv(), System.currentTimeMillis());
        } catch (IllegalArgumentException e) {
            System.err.println("loadgen: " + e.getMessage());
            System.err.print(LoadGenArgs.USAGE);
            System.exit(2);
            return;
        }
        try {
            System.exit(new LoadGenMain(args).run());
        } catch (java.net.ConnectException e) {
            System.err.println("loadgen: cannot connect to " + args.apis() + " (is the compose stack up?)");
            System.exit(1);
        } catch (SQLException e) {
            System.err.println("loadgen: database error " + e.getSQLState() + ": " + e.getMessage());
            System.exit(1);
        }
    }

    int run() throws Exception {
        List<String> tenants = args.tenantIds();
        out("# loadgen compose " + args.scenario() + "\n");
        out("Started " + Instant.now() + "; nodes " + args.apis() + "; tenants " + tenants.get(0) + ".."
                + tenants.get(tenants.size() - 1) + " (cap " + args.cap() + ")\n");

        String operator = token(args.prefix() + "-ops", "OPERATOR");
        for (String t : tenants) {
            createTenant(operator, t);
            adminTokens.put(t, token(t, "ADMIN"));
        }

        // plan the jobs: tenant and runAt of each
        Instant start = Instant.now().plusSeconds(args.leadSec()).truncatedTo(ChronoUnit.MILLIS);
        int n = args.totalJobs();
        String[] jobTenant = new String[n];
        Instant[] runAt = new Instant[n];
        SplittableRandom rnd = new SplittableRandom();
        if (args.scenario().equals("burst")) {
            for (int i = 0; i < n; i++) {
                jobTenant[i] = i < args.burstA() ? tenants.get(0) : tenants.get(1);
                runAt[i] = start;
            }
        } else {
            long windowMs = args.windowSec() * 1000L;
            for (int i = 0; i < n; i++) {
                jobTenant[i] = tenants.get(i % tenants.size());
                runAt[i] = start.plusMillis(rnd.nextLong(windowMs + 1));
            }
        }

        long t0 = System.nanoTime();
        CreateResult created = createJobs(jobTenant, runAt);
        double createS = (System.nanoTime() - t0) / 1e9;
        Instant createdBy = Instant.now();
        MarkdownTable c = new MarkdownTable("metric", "value");
        c.row("jobs requested", n);
        c.row("jobs created", created.ok());
        c.row("create failures", created.failed() + (created.firstError() == null ? "" : " (first: "
                + created.firstError() + ")"));
        c.row("create retries", created.retries());
        c.row("create time (s)", f1(createS));
        c.row("create rate (jobs/s)", f1(created.ok() / Math.max(createS, 1e-9)));
        c.row("first due time", start);
        c.row("creation finished before the first due time by (s)", f1(Duration.between(createdBy, start).toMillis()
                / 1000.0));
        c.row("tokens", localTokens ? "minted locally (HS256), /dev/token absent" : "POST /dev/token");
        out("## Load\n\n" + c);
        if (created.ok() == 0) {
            out("nothing was created; stopping");
            return 1;
        }

        Instant lastDue = args.scenario().equals("burst") ? start : start.plusSeconds(args.windowSec());
        Instant deadline = lastDue.plusSeconds(args.waitSec());
        String[] tenantArr = tenants.toArray(String[]::new);
        try (Connection db = DriverManager.getConnection(args.jdbc())) {
            awaitTerminal(db, tenantArr, created.ok(), deadline);
            List<RunSample> samples = RunSample.load(db, "tenant_id = ANY (?)", (Object) tenantArr);
            out("## Result\n\n" + RunTables.summary(samples, args.scenario().equals("burst") ? 0 : args.windowSec()));
            long startUs = start.toEpochMilli() * 1000;
            if (args.scenario().equals("burst")) {
                out("## Burst and fairness (all due at T = " + start + ")\n\n"
                        + RunTables.burst(samples, tenantArr[0], tenantArr[1], startUs));
            } else {
                MarkdownTable lag = LagReport.table("runs");
                RunTables.lag(samples).row(lag, "all");
                out("## Fire lag (started_at - scheduled_at, DB clock)\n\n" + lag);
                out("## Per minute of the window\n\n" + RunTables.perMinute(samples, startUs));
            }
            out("## Sink (attempts of these runs, from sink.attempt_log)\n\n" + sinkStats(db, samples));
            Path dir = Path.of(args.outDir());
            String stamp = STAMP.format(Instant.now());
            Files.createDirectories(dir);
            Path md = dir.resolve(args.scenario() + "-compose-" + stamp + ".md");
            Files.writeString(md, report.toString(), StandardCharsets.UTF_8);
            RunSample.writeCsv(dir.resolve(args.scenario() + "-compose-" + stamp + "-lags.csv"), samples);
            System.out.println("report written to " + md.toAbsolutePath());
            return RunTables.outcome(samples).notTerminal() == 0 ? 0 : 1;
        }
    }

    // ------------------------------------------------------------------ tokens and tenants

    private String token(String tenant, String role) throws IOException, InterruptedException {
        if (!localTokens) {
            ObjectNode body = mapper.createObjectNode().put("tenant", tenant).put("role", role)
                    .put("subject", "loadgen-" + role.toLowerCase());
            HttpResponse<String> r = send(HttpRequest.newBuilder(args.apis().get(0).resolve("/dev/token"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body.toString())).build());
            if (r.statusCode() == 200) {
                return mapper.readTree(r.body()).path("token").asText();
            }
            if (args.hmacSecret() == null) {
                throw new IllegalStateException("POST /dev/token -> " + r.statusCode() + " (is the dev profile active? "
                        + "otherwise pass --hmac-secret)");
            }
            localTokens = true;
        }
        return Hs256.token("loadgen-" + role.toLowerCase(), tenant, role, Instant.now(), Duration.ofHours(6),
                args.hmacSecret());
    }

    private void createTenant(String operatorToken, String tenant) throws IOException, InterruptedException {
        ObjectNode body = mapper.createObjectNode().put("id", tenant).put("timeZone", "UTC").put("cap", args.cap());
        HttpResponse<String> r = send(HttpRequest.newBuilder(args.apis().get(0).resolve("/api/v1/tenants"))
                .header("Content-Type", "application/json").header("Authorization", "Bearer " + operatorToken)
                .POST(HttpRequest.BodyPublishers.ofString(body.toString())).build());
        if (r.statusCode() != 201 && r.statusCode() != 409) {
            throw new IllegalStateException("POST /api/v1/tenants " + tenant + " -> " + r.statusCode() + " "
                    + code(r.body()));
        }
    }

    // ------------------------------------------------------------------ jobs

    record CreateResult(int ok, int failed, int retries, String firstError) {
    }

    private CreateResult createJobs(String[] tenant, Instant[] runAt) throws InterruptedException {
        Semaphore inFlight = new Semaphore(args.concurrency());
        AtomicInteger ok = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        AtomicInteger retries = new AtomicInteger();
        AtomicReference<String> firstError = new AtomicReference<>();
        AtomicLong lastPrint = new AtomicLong(System.nanoTime());
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < tenant.length; i++) {
                inFlight.acquire();
                int idx = i;
                pool.submit(() -> {
                    try {
                        createJob(idx, tenant[idx], runAt[idx], retries);
                        int done = ok.incrementAndGet();
                        long now = System.nanoTime();
                        long last = lastPrint.get();
                        if (now - last > 5_000_000_000L && lastPrint.compareAndSet(last, now)) {
                            System.out.printf("loadgen: created %d / %d jobs%n", done, tenant.length);
                        }
                    } catch (Exception e) {
                        failed.incrementAndGet();
                        firstError.compareAndSet(null, e.getMessage());
                    } finally {
                        inFlight.release();
                    }
                    return null;
                });
            }
        }
        return new CreateResult(ok.get(), failed.get(), retries.get(), firstError.get());
    }

    private String createJob(int idx, String tenant, Instant runAt, AtomicInteger retries) throws Exception {
        ObjectNode body = mapper.createObjectNode();
        body.put("kind", "ONE_OFF").put("handler", "PICKUP_ALERT").put("runAt", runAt.toString());
        ObjectNode payload = body.putObject("payload");
        payload.putArray("employeeIds").add("emp-0001");
        payload.put("messageTemplate", TEMPLATE);
        body.put("priority", 5).put("maxRunTime", "PT15M").put("misfire", "FIRE_NOW").put("maxAttempts", 5);
        String json = body.toString();
        String key = args.prefix() + ":job:" + idx;
        URI api = args.apis().get(idx % args.apis().size());
        Exception last = null;
        for (int attempt = 0; attempt < 6; attempt++) {
            if (attempt > 0) {
                retries.incrementAndGet();
                Thread.sleep(Math.min(5_000, 200L << attempt));
            }
            try {
                HttpResponse<String> r = send(HttpRequest.newBuilder(api.resolve("/api/v1/jobs"))
                        .timeout(Duration.ofSeconds(30))
                        .header("Content-Type", "application/json")
                        .header("Authorization", "Bearer " + adminTokens.get(tenant))
                        .header("Idempotency-Key", key)
                        .POST(HttpRequest.BodyPublishers.ofString(json)).build());
                int s = r.statusCode();
                if (s == 201 || s == 200) {
                    return mapper.readTree(r.body()).path("id").asText();
                }
                if (s < 500 && s != 429) {
                    throw new IllegalStateException("POST /api/v1/jobs -> " + s + " " + code(r.body()));
                }
                last = new IllegalStateException("POST /api/v1/jobs -> " + s + " " + code(r.body()));
            } catch (IOException e) {
                last = e;
                api = args.apis().get((idx + attempt + 1) % args.apis().size()); // try another node
            }
        }
        throw last;
    }

    // ------------------------------------------------------------------ waiting and reading back

    private void awaitTerminal(Connection db, String[] tenants, int expected, Instant deadline)
            throws SQLException, InterruptedException {
        long lastPrint = 0;
        while (true) {
            long[] p = RunSample.progress(db, "tenant_id = ANY (?)", (Object) tenants);
            boolean done = p[1] >= expected;
            if (done || Instant.now().isAfter(deadline)) {
                System.out.printf("loadgen: %s: runs=%d terminal=%d running=%d started=%d (expected %d)%n",
                        done ? "done" : "deadline reached", p[0], p[1], p[2], p[3], expected);
                return;
            }
            if (System.nanoTime() - lastPrint > 15_000_000_000L) {
                System.out.printf("loadgen: waiting: runs=%d terminal=%d running=%d started=%d of %d%n",
                        p[0], p[1], p[2], p[3], expected);
                lastPrint = System.nanoTime();
            }
            Thread.sleep(2_000);
        }
    }

    /** Counts from the sink's attempt log for these runs (compose: the sink shares the database). */
    private String sinkStats(Connection db, List<RunSample> samples) {
        Long[] runIds = samples.stream().map(RunSample::id).toArray(Long[]::new);
        try (PreparedStatement ps = db.prepareStatement("""
                SELECT count(*) FILTER (WHERE outcome = 'DELIVERED'),
                       count(DISTINCT idem_key) FILTER (WHERE outcome = 'DELIVERED'),
                       count(*) FILTER (WHERE outcome = 'ALREADY_DELIVERED'),
                       count(*) FILTER (WHERE outcome = 'STALE_TOKEN')
                  FROM sink.attempt_log WHERE run_id = ANY (?)""")) {
            ps.setArray(1, db.createArrayOf("int8", runIds));
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                long delivered = rs.getLong(1);
                long distinct = rs.getLong(2);
                MarkdownTable t = new MarkdownTable("metric", "value");
                t.row("delivered (attempts with outcome DELIVERED)", delivered);
                t.row("delivered distinct keys", distinct);
                t.row("already delivered (absorbed repeats)", rs.getLong(3));
                t.row("stale token rejected", rs.getLong(4));
                t.row("duplicates = delivered - distinct (must be 0)", delivered - distinct);
                return t.toString();
            }
        } catch (SQLException e) {
            return "sink stats unavailable (" + e.getSQLState() + "): the sink schema is not in this database\n";
        }
    }

    // ------------------------------------------------------------------ helpers

    private HttpResponse<String> send(HttpRequest r) throws IOException, InterruptedException {
        return http.send(r, HttpResponse.BodyHandlers.ofString());
    }

    private String code(String body) {
        try {
            JsonNode n = mapper.readTree(body);
            return n.path("code").asText(n.path("title").asText(""));
        } catch (IOException | RuntimeException e) {
            return "";
        }
    }

    private void out(String s) {
        String text = s.endsWith("\n") ? s + "\n" : s + "\n\n";
        System.out.print(text);
        report.append(text);
    }

    /** Local HS256 JWT in the scheduler's claim format ({@code sub}, {@code tenant}, {@code roles}, iat, exp). */
    static final class Hs256 {

        private Hs256() {
        }

        static String token(String subject, String tenant, String role, Instant now, Duration ttl, String secret) {
            ObjectMapper m = new ObjectMapper();
            ObjectNode header = m.createObjectNode().put("alg", "HS256").put("typ", "JWT");
            ObjectNode claims = m.createObjectNode().put("sub", subject).put("tenant", tenant)
                    .put("iat", now.getEpochSecond()).put("exp", now.plus(ttl).getEpochSecond());
            claims.putArray("roles").add(role);
            String signingInput = b64(header.toString().getBytes(StandardCharsets.UTF_8)) + "."
                    + b64(claims.toString().getBytes(StandardCharsets.UTF_8));
            try {
                javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
                mac.init(new javax.crypto.spec.SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
                return signingInput + "." + b64(mac.doFinal(signingInput.getBytes(StandardCharsets.US_ASCII)));
            } catch (java.security.GeneralSecurityException e) {
                throw new IllegalStateException(e);
            }
        }

        private static String b64(byte[] b) {
            return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(b);
        }
    }
}
