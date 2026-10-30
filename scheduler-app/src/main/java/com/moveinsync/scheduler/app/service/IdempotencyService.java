package com.moveinsync.scheduler.app.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.moveinsync.scheduler.app.error.ConflictException;
import com.moveinsync.scheduler.app.error.ErrorCodes;
import com.moveinsync.scheduler.app.repository.idempotency.IdempotencyRepository;
import com.moveinsync.scheduler.app.security.Caller;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Retry-safe writes (docs/design/architecture.md section 13). With an {@code Idempotency-Key}, the action and the
 * stored response commit in <b>one transaction</b>, under an advisory lock on (namespace, key):
 * <ul>
 *   <li>same key, same request → the stored status, {@code Location} and body are replayed
 *       (header {@code Idempotent-Replayed: true}) and the action does not run again;</li>
 *   <li>same key, different request (method, path or canonical body) → 409
 *       {@code IDEMPOTENCY_KEY_REUSED};</li>
 *   <li>two identical requests at the same moment → the second waits for the lock, then
 *       replays the first one's response: one action.</li>
 * </ul>
 * Failed actions (4xx/5xx exceptions) roll back and store nothing, so they can be retried.
 * Records are honoured for {@link #RETENTION}; an older record with the same key is replaced.
 */
@Service
public class IdempotencyService {

    public static final Duration RETENTION = Duration.ofHours(24);
    public static final String REPLAYED_HEADER = "Idempotent-Replayed";

    private final IdempotencyRepository store;
    private final TransactionTemplate tx;
    private final ObjectMapper mapper;

    public IdempotencyService(IdempotencyRepository store, PlatformTransactionManager txManager, ObjectMapper mapper) {
        this.store = store;
        this.tx = new TransactionTemplate(txManager);
        this.mapper = mapper;
    }

    /**
     * Runs {@code action} at most once per (caller namespace, key).
     *
     * @param key       the {@code Idempotency-Key} header, or null to run the action plainly
     * @param operation method and concrete path, e.g. {@code POST /api/v1/tenants/acme/pause}
     * @param request   request body (or null); hashed in canonical form with the operation
     */
    public ResponseEntity<?> execute(Caller caller, String key, String operation, Object request,
                                     Supplier<? extends ResponseEntity<?>> action) {
        return execute(caller, key, operation, request, action, UnaryOperator.identity());
    }

    /**
     * As {@link #execute(Caller, String, String, Object, Supplier)}, with {@code onReplay}
     * adjusting a replayed body (e.g. a trigger replay reports {@code duplicate: true}).
     */
    public ResponseEntity<?> execute(Caller caller, String key, String operation, Object request,
                                     Supplier<? extends ResponseEntity<?>> action,
                                     UnaryOperator<JsonNode> onReplay) {
        if (key == null) {
            return action.get();
        }
        String namespace = caller.idempotencyNamespace();
        String hash = requestHash(operation, request);
        return tx.execute(status -> {
            store.lock(namespace, key);
            var stored = store.find(namespace, key, RETENTION);
            if (stored.isPresent()) {
                if (!stored.get().requestHash().equals(hash)) {
                    throw new ConflictException(ErrorCodes.IDEMPOTENCY_KEY_REUSED,
                            "Idempotency-Key was already used for a different request");
                }
                return replay(stored.get().responseJson(), onReplay);
            }
            ResponseEntity<?> response = action.get();
            store.save(namespace, key, hash, serialise(response));
            return response;
        });
    }

    // ------------------------------------------------------------------ stored form

    private String serialise(ResponseEntity<?> response) {
        ObjectNode stored = mapper.createObjectNode();
        stored.put("status", response.getStatusCode().value());
        URI location = response.getHeaders().getLocation();
        if (location != null) {
            stored.put("location", location.toString());
        }
        stored.set("body", mapper.valueToTree(response.getBody()));
        return write(stored);
    }

    private ResponseEntity<JsonNode> replay(String json, UnaryOperator<JsonNode> onReplay) {
        JsonNode stored = read(json);
        ResponseEntity.BodyBuilder builder = ResponseEntity.status(stored.path("status").asInt())
                .header(REPLAYED_HEADER, "true");
        if (stored.hasNonNull("location")) {
            builder.header(HttpHeaders.LOCATION, stored.get("location").asText());
        }
        return builder.body(onReplay.apply(stored.get("body")));
    }

    // ------------------------------------------------------------------ request hash

    /** SHA-256 over the operation and the canonical JSON (keys sorted) of the body. */
    String requestHash(String operation, Object request) {
        String body = request == null ? "" : write(canonical(mapper.valueToTree(request)));
        try {
            MessageDigest sha = MessageDigest.getInstance("SHA-256");
            sha.update(operation.getBytes(StandardCharsets.UTF_8));
            sha.update((byte) 0);
            sha.update(body.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(sha.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    /** Copy of {@code node} with object keys sorted at every level, so key order does not matter. */
    private JsonNode canonical(JsonNode node) {
        if (node.isObject()) {
            ObjectNode sorted = mapper.createObjectNode();
            List<String> names = new ArrayList<>();
            node.fieldNames().forEachRemaining(names::add);
            names.sort(null);
            for (String name : names) {
                sorted.set(name, canonical(node.get(name)));
            }
            return sorted;
        }
        if (node.isArray()) {
            ArrayNode copy = mapper.createArrayNode();
            node.forEach(child -> copy.add(canonical(child)));
            return copy;
        }
        return node;
    }

    private String write(JsonNode node) {
        try {
            return mapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("cannot serialise JSON", e);
        }
    }

    private JsonNode read(String json) {
        try {
            return mapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("stored idempotent response is not valid JSON", e);
        }
    }
}
