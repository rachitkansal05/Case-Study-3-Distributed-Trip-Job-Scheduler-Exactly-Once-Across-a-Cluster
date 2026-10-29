package com.moveinsync.scheduler.loadgen;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Command line of {@link LoadGenMain}. Options are {@code --name value} or {@code --name=value}.
 *
 * @param apis        scheduler base URLs, used round robin
 * @param jobs        spread: number of one-off jobs
 * @param tenants     spread: number of tenants
 * @param cap         concurrency cap of every created tenant
 * @param windowSec   spread: runAt uniform over [start, start + window]
 * @param leadSec     seconds from now to the first runAt (spread) or to T (burst); creation must fit in it
 * @param jdbc        JDBC URL of the scheduler database (lag is read from job_run)
 * @param scenario    {@code spread} or {@code burst}
 * @param burstA      burst: jobs of tenant A, all due at T
 * @param burstB      burst: jobs of tenant B, all due at T
 * @param concurrency concurrent HTTP requests while creating jobs
 * @param prefix      tenant id prefix; unique per run so reruns do not mix
 * @param hmacSecret  if {@code POST /dev/token} is absent (non-dev profile), tokens are minted
 *                    locally with this HS256 secret; null disables the fallback
 * @param waitSec     how long to wait for the last runs after the last due time
 * @param outDir      where the Markdown report and the lag CSV are written
 */
public record LoadGenArgs(List<URI> apis, int jobs, int tenants, int cap, int windowSec, int leadSec, String jdbc,
                          String scenario, int burstA, int burstB, int concurrency, String prefix, String hmacSecret,
                          int waitSec, String outDir) {

    public static final String USAGE = """
            usage: java -jar loadgen.jar [options]
              --api URL[,URL...]   scheduler nodes (default http://localhost:8081,http://localhost:8082,http://localhost:8083)
              --scenario NAME      spread | burst (default spread)
              --jobs N             spread: one-off PICKUP_ALERT jobs (default 100000)
              --tenants N          spread: tenants (default 50)
              --cap N              cap of each created tenant (default 32)
              --window SEC         spread: runAt uniform over this window (default 600)
              --lead SEC           seconds until the first runAt / burst instant (default 120)
              --burst-a N          burst: jobs of tenant A (default 50000)
              --burst-b N          burst: jobs of tenant B (default 20)
              --jdbc URL           scheduler DB, quote it in a shell
                                   (default jdbc:postgresql://localhost:5432/scheduler?user=scheduler&password=scheduler)
              --concurrency N      concurrent create requests (default 64)
              --prefix ID          tenant id prefix (default lg<base36 time>)
              --hmac-secret S      local HS256 token fallback when /dev/token is absent
                                   (default: env SCHEDULER_JWT_HMAC_SECRET, else none)
              --wait SEC           wait after the last due time (default 300)
              --out DIR            report directory (default target/bench)
            """;

    private static final Set<String> KNOWN = Set.of("api", "scenario", "jobs", "tenants", "cap", "window", "lead",
            "burst-a", "burst-b", "jdbc", "concurrency", "prefix", "hmac-secret", "wait", "out");

    public LoadGenArgs {
        if (apis.isEmpty()) {
            throw new IllegalArgumentException("--api needs at least one URL");
        }
        if (!scenario.equals("spread") && !scenario.equals("burst")) {
            throw new IllegalArgumentException("--scenario must be spread or burst, was " + scenario);
        }
        positive("jobs", jobs);
        positive("tenants", tenants);
        positive("window", windowSec);
        positive("concurrency", concurrency);
        positive("burst-a", burstA);
        positive("burst-b", burstB);
        if (cap < 1 || leadSec < 0 || waitSec < 0) {
            throw new IllegalArgumentException("--cap must be >= 1, --lead and --wait >= 0");
        }
        if (!prefix.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,40}")) {
            throw new IllegalArgumentException("--prefix must be letters, digits, . _ - (max 41 characters)");
        }
        apis = List.copyOf(apis);
    }

    /** Parses {@code args}; {@code env} supplies SCHEDULER_JWT_HMAC_SECRET; {@code nowMillis} the default prefix. */
    public static LoadGenArgs parse(String[] args, Map<String, String> env, long nowMillis) {
        Map<String, String> o = new LinkedHashMap<>();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (!a.startsWith("--") || a.length() == 2) {
                throw new IllegalArgumentException("unexpected argument: " + a);
            }
            String name;
            String value;
            int eq = a.indexOf('=');
            if (eq > 0) {
                name = a.substring(2, eq);
                value = a.substring(eq + 1);
            } else {
                name = a.substring(2);
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("--" + name + " needs a value");
                }
                value = args[++i];
            }
            if (!KNOWN.contains(name)) {
                throw new IllegalArgumentException("unknown option --" + name);
            }
            o.put(name, value);
        }
        List<URI> apis = new ArrayList<>();
        for (String s : o.getOrDefault("api", "http://localhost:8081,http://localhost:8082,http://localhost:8083")
                .split(",")) {
            String t = s.trim();
            if (!t.isEmpty()) {
                URI u = URI.create(t.endsWith("/") ? t.substring(0, t.length() - 1) : t);
                if (u.getScheme() == null || u.getHost() == null) {
                    throw new IllegalArgumentException("--api: not an absolute URL: " + t);
                }
                apis.add(u);
            }
        }
        String secret = o.containsKey("hmac-secret") ? o.get("hmac-secret") : env.get("SCHEDULER_JWT_HMAC_SECRET");
        return new LoadGenArgs(apis,
                integer(o, "jobs", 100_000),
                integer(o, "tenants", 50),
                integer(o, "cap", 32),
                integer(o, "window", 600),
                integer(o, "lead", 120),
                o.getOrDefault("jdbc", "jdbc:postgresql://localhost:5432/scheduler?user=scheduler&password=scheduler"),
                o.getOrDefault("scenario", "spread").toLowerCase(Locale.ROOT),
                integer(o, "burst-a", 50_000),
                integer(o, "burst-b", 20),
                integer(o, "concurrency", 64),
                o.getOrDefault("prefix", "lg" + Long.toString(nowMillis / 1000, 36)),
                secret == null || secret.isBlank() ? null : secret,
                integer(o, "wait", 300),
                o.getOrDefault("out", "target/bench"));
    }

    /** Tenant ids of this run, in creation order. */
    public List<String> tenantIds() {
        List<String> ids = new ArrayList<>();
        if (scenario.equals("burst")) {
            ids.add(prefix + "-a");
            ids.add(prefix + "-b");
        } else {
            for (int i = 0; i < tenants; i++) {
                ids.add(String.format(Locale.ROOT, "%s-t%02d", prefix, i));
            }
        }
        return ids;
    }

    /** Jobs this run creates. */
    public int totalJobs() {
        return scenario.equals("burst") ? burstA + burstB : jobs;
    }

    private static int integer(Map<String, String> o, String name, int def) {
        String v = o.get(name);
        if (v == null) {
            return def;
        }
        try {
            return Integer.parseInt(v.trim().replace("_", ""));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("--" + name + " must be an integer, was " + v);
        }
    }

    private static void positive(String name, int v) {
        if (v < 1) {
            throw new IllegalArgumentException("--" + name + " must be >= 1");
        }
    }
}
