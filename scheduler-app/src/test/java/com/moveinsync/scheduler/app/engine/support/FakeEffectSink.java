package com.moveinsync.scheduler.app.engine.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.core.domain.DeliveryOutcome;
import com.moveinsync.scheduler.core.domain.FencingToken;
import com.moveinsync.scheduler.core.policy.PermanentJobException;
import com.moveinsync.scheduler.core.policy.RetryableJobException;
import com.moveinsync.scheduler.core.ports.EffectSink;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.LongFunction;

/**
 * In-memory {@link EffectSink} with exactly the semantics of docs/sink-api.md §1, "What the sink
 * does":
 *
 * <ol>
 *   <li>Validation (like the sink's 400): the key must be 1-256 visible ASCII characters and the
 *       payload a JSON object, else {@link PermanentJobException} {@code SINK_REJECTED_400}. Nothing
 *       is written.
 *   <li>Faults, before any write: {@link #blockSends()} gate, {@link #setLatency} sleep, then {@link
 *       #setFailureRate} ({@link RetryableJobException} {@code SINK_5XX}).
 *   <li>One atomic decision under one lock: a token lower than the run's fence → {@code
 *       STALE_TOKEN} (the fence check comes before the key check); otherwise the {@link
 *       #setAdmissionValidator admission validator} may still reject it as {@code STALE_TOKEN};
 *       otherwise the fence rises to the token (equal tokens are accepted) and the global key
 *       decides {@code DELIVERED} (first) or {@code ALREADY_DELIVERED} (repeat). Every decision is
 *       appended to the attempt log.
 *   <li>Accepted calls are counted in flight for their {@link #setGroupOf group} and sleep {@link
 *       #setHoldAfterAdmission} while counted.
 * </ol>
 *
 * <p>Thread-safe; no static state.
 */
public final class FakeEffectSink implements EffectSink {

    /** Group used for in-flight tracking when no {@link #setGroupOf group function} is set. */
    public static final String DEFAULT_GROUP = "*";

    /** One logged (validated, not faulted) call. */
    public record Attempt(long runId, String idemKey, FencingToken token, DeliveryOutcome outcome, Instant at) {}

    /** One recorded delivery (the first accepted call of a key). */
    public record Delivery(long runId, String idemKey, FencingToken token, Instant at) {}

    private final Clock clock;
    private final ObjectMapper mapper = new ObjectMapper();

    private final Object lock = new Object();
    private final Map<Long, FencingToken> fences = new HashMap<>();
    private final Map<String, Delivery> deliveries = new LinkedHashMap<>();
    private final Map<String, Integer> deliveriesPerKey = new HashMap<>();
    private final List<Attempt> attempts = new ArrayList<>();
    private long maxAcceptedEpoch;
    private int staleRejections;

    private final Map<String, AtomicInteger> inFlight = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> maxInFlight = new ConcurrentHashMap<>();

    private volatile double failureRate;
    private volatile Duration latency = Duration.ZERO;
    private volatile Duration holdAfterAdmission = Duration.ZERO;
    private volatile Long highestEpochOverride;
    private volatile BiPredicate<Long, FencingToken> admissionValidator;
    private volatile LongFunction<String> groupOf;
    private volatile CountDownLatch gate;

    /** Uses the UTC system clock for attempt times. */
    public FakeEffectSink() {
        this(Clock.systemUTC());
    }

    /** @param clock clock for attempt and delivery times */
    public FakeEffectSink(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public DeliveryOutcome send(long runId, String idemKey, FencingToken token, String payloadJson) {
        Objects.requireNonNull(token, "token");
        validate(runId, idemKey, payloadJson);
        awaitGate();
        sleep(latency);
        double rate = failureRate;
        if (rate > 0 && ThreadLocalRandom.current().nextDouble() < rate) {
            throw new RetryableJobException("SINK_5XX", "injected sink fault");
        }
        DeliveryOutcome outcome = decide(runId, idemKey, token);
        if (outcome.isSuccess()) {
            holdInFlight(runId);
        }
        return outcome;
    }

    private DeliveryOutcome decide(long runId, String idemKey, FencingToken token) {
        synchronized (lock) {
            Instant now = clock.instant();
            FencingToken fence = fences.get(runId);
            BiPredicate<Long, FencingToken> validator = admissionValidator;
            DeliveryOutcome outcome;
            if ((fence != null && token.compareTo(fence) < 0)
                    || (validator != null && !validator.test(runId, token))) {
                staleRejections++;
                outcome = DeliveryOutcome.STALE_TOKEN;
            } else {
                if (fence == null || token.isNewerThan(fence)) {
                    fences.put(runId, token);
                }
                maxAcceptedEpoch = Math.max(maxAcceptedEpoch, token.epoch());
                if (deliveries.containsKey(idemKey)) {
                    outcome = DeliveryOutcome.ALREADY_DELIVERED;
                } else {
                    deliveries.put(idemKey, new Delivery(runId, idemKey, token, now));
                    deliveriesPerKey.merge(idemKey, 1, Integer::sum);
                    outcome = DeliveryOutcome.DELIVERED;
                }
            }
            attempts.add(new Attempt(runId, idemKey, token, outcome, now));
            return outcome;
        }
    }

    private void validate(long runId, String idemKey, String payloadJson) {
        boolean keyOk = idemKey != null && !idemKey.isEmpty() && idemKey.length() <= 256
                && idemKey.chars().allMatch(c -> c >= 0x21 && c <= 0x7E);
        if (runId < 1 || !keyOk || !isJsonObject(payloadJson)) {
            throw new PermanentJobException("SINK_REJECTED_400", "sink answered 400");
        }
    }

    private boolean isJsonObject(String json) {
        if (json == null) {
            return false;
        }
        try {
            JsonNode node = mapper.readTree(json);
            return node != null && node.isObject();
        } catch (IOException e) {
            return false;
        }
    }

    private void holdInFlight(long runId) {
        LongFunction<String> groups = groupOf;
        String group = groups == null ? DEFAULT_GROUP : groups.apply(runId);
        AtomicInteger current = inFlight.computeIfAbsent(group, g -> new AtomicInteger());
        int now = current.incrementAndGet();
        maxInFlight.computeIfAbsent(group, g -> new AtomicInteger()).accumulateAndGet(now, Math::max);
        try {
            sleep(holdAfterAdmission);
        } finally {
            current.decrementAndGet();
        }
    }

    private void awaitGate() {
        CountDownLatch g = gate;
        if (g == null) {
            return;
        }
        try {
            g.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RetryableJobException("INTERRUPTED", "interrupted while blocked");
        }
    }

    private static void sleep(Duration d) {
        if (d.isZero() || d.isNegative()) {
            return;
        }
        try {
            Thread.sleep(d);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RetryableJobException("INTERRUPTED", "interrupted while calling the sink");
        }
    }

    /** {@link #setHighestEpochOverride override} if set, else the max accepted epoch, else 0. */
    @Override
    public long highestAcceptedEpoch() {
        Long override = highestEpochOverride;
        if (override != null) {
            return override;
        }
        synchronized (lock) {
            return maxAcceptedEpoch;
        }
    }

    // ---- inspection ------------------------------------------------------------------------

    /** Number of distinct delivered keys. */
    public int distinctDeliveries() {
        synchronized (lock) {
            return deliveries.size();
        }
    }

    /** Deliveries recorded for one run, in delivery order. */
    public List<Delivery> deliveriesFor(long runId) {
        synchronized (lock) {
            return deliveries.values().stream().filter(d -> d.runId() == runId).toList();
        }
    }

    /** The delivery of a key, or null. */
    public Delivery deliveryOf(String idemKey) {
        synchronized (lock) {
            return deliveries.get(idemKey);
        }
    }

    /** Copy of the attempt log, in decision order. */
    public List<Attempt> attempts() {
        synchronized (lock) {
            return List.copyOf(attempts);
        }
    }

    /** Number of logged attempts. */
    public int attemptCount() {
        synchronized (lock) {
            return attempts.size();
        }
    }

    /** Attempts answered {@code STALE_TOKEN}. */
    public int staleRejections() {
        synchronized (lock) {
            return staleRejections;
        }
    }

    /** Highest delivery count of any key, from a counter separate from the delivery map; 0 if none. */
    public int maxDeliveriesPerKey() {
        synchronized (lock) {
            return deliveriesPerKey.values().stream().mapToInt(Integer::intValue).max().orElse(0);
        }
    }

    /** Current fence of a run, or null. */
    public FencingToken fenceOf(long runId) {
        synchronized (lock) {
            return fences.get(runId);
        }
    }

    /** Accepted calls currently held in flight for a group. */
    public int inFlight(String group) {
        AtomicInteger n = inFlight.get(group);
        return n == null ? 0 : n.get();
    }

    /** Highest in-flight count ever seen for a group. */
    public int maxInFlight(String group) {
        AtomicInteger n = maxInFlight.get(group);
        return n == null ? 0 : n.get();
    }

    // ---- configuration ---------------------------------------------------------------------

    /** Probability in [0, 1] of an injected {@code SINK_5XX} before any write. */
    public void setFailureRate(double rate) {
        if (!(rate >= 0.0 && rate <= 1.0)) {
            throw new IllegalArgumentException("failure rate must be in [0, 1]");
        }
        this.failureRate = rate;
    }

    /** Delay applied before the decision (and before fault injection). */
    public void setLatency(Duration before) {
        this.latency = Objects.requireNonNull(before, "before");
    }

    /** Fixed answer for {@link #highestAcceptedEpoch()}; null to measure again. */
    public void setHighestEpochOverride(Long epoch) {
        this.highestEpochOverride = epoch;
    }

    /**
     * Extra check for calls that pass the fence, inside the atomic decision; false → {@code
     * STALE_TOKEN} without raising the fence. Null removes it.
     */
    public void setAdmissionValidator(BiPredicate<Long, FencingToken> validator) {
        this.admissionValidator = validator;
    }

    /** Maps a run id to its in-flight group; null puts every run in {@value #DEFAULT_GROUP}. */
    public void setGroupOf(LongFunction<String> runIdToGroup) {
        this.groupOf = runIdToGroup;
    }

    /** Time an accepted call stays in flight after its decision. */
    public void setHoldAfterAdmission(Duration hold) {
        this.holdAfterAdmission = Objects.requireNonNull(hold, "hold");
    }

    /** Makes new calls wait (before latency, faults and the decision) until {@link #unblockSends()}. */
    public void blockSends() {
        synchronized (lock) {
            if (gate == null) {
                gate = new CountDownLatch(1);
            }
        }
    }

    /** Releases all blocked calls. */
    public void unblockSends() {
        CountDownLatch g;
        synchronized (lock) {
            g = gate;
            gate = null;
        }
        if (g != null) {
            g.countDown();
        }
    }
}
