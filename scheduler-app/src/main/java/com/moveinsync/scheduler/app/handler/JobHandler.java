package com.moveinsync.scheduler.app.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.policy.PermanentJobException;
import com.moveinsync.scheduler.core.policy.RetryableJobException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

/**
 * Business handler of one {@link HandlerType} (plan section 07, "Execute"). {@link #handle} is the
 * template: parse and validate the payload, run {@link #execute}, then classify failures.
 *
 * <ul>
 *   <li>Parse or validation failures → {@link PermanentJobException} {@value #INVALID_PAYLOAD}; the
 *       message names at most the field, never a value.
 *   <li>{@link LeaseLostException}, {@link RetryableJobException}, {@link PermanentJobException}
 *       and {@link InterruptedException} propagate unchanged.
 *   <li>{@link IOException}/{@link UncheckedIOException} → {@link RetryableJobException} {@value
 *       #IO_ERROR}.
 *   <li>Anything else propagates; the engine's {@code FailureClassifier} treats it as retryable.
 * </ul>
 *
 * <p>Implementations are stateless and thread-safe; one instance serves all runs.
 *
 * @param <P> validated payload type
 */
public abstract class JobHandler<P> {

    /** Error code for a payload that cannot be parsed or fails validation. */
    public static final String INVALID_PAYLOAD = "INVALID_PAYLOAD";

    /** Error code for an I/O failure inside a handler. */
    public static final String IO_ERROR = "IO_ERROR";

    private final ObjectMapper mapper;
    private final ObjectReader strictReader;

    /** @param mapper shared Jackson mapper */
    protected JobHandler(ObjectMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.strictReader = mapper.reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    }

    /** Handler type this instance serves. */
    public abstract HandlerType type();

    /**
     * Runs one attempt.
     *
     * @throws InterruptedException when the worker is interrupted (lease lost or max run time)
     */
    public final void handle(EffectContext ctx) throws InterruptedException {
        Objects.requireNonNull(ctx, "ctx");
        P payload = parseSafely(ctx.payloadJson());
        try {
            execute(ctx, payload);
        } catch (UncheckedIOException e) {
            throw new RetryableJobException(IO_ERROR, "I/O error in handler " + type(), e);
        } catch (Exception e) {
            if (e instanceof IOException) {
                throw new RetryableJobException(IO_ERROR, "I/O error in handler " + type(), e);
            }
            throw e;
        }
    }

    /**
     * Parses and validates the job payload. Throw {@link #invalid(String)} for bad input; any other
     * exception is also reported as {@value #INVALID_PAYLOAD}.
     */
    protected abstract P parsePayload(String payloadJson);

    /**
     * Performs the effects through {@code ctx.send}.
     *
     * @throws InterruptedException must be propagated, never swallowed
     */
    protected abstract void execute(EffectContext ctx, P payload) throws InterruptedException;

    private P parseSafely(String payloadJson) {
        try {
            return Objects.requireNonNull(parsePayload(payloadJson), "payload");
        } catch (PermanentJobException e) {
            throw e;
        } catch (RuntimeException e) {
            // No cause: parser messages can quote the input.
            throw invalid("payload could not be parsed");
        }
    }

    /** The shared mapper. */
    protected final ObjectMapper mapper() {
        return mapper;
    }

    /** {@value #INVALID_PAYLOAD} with a message that must not contain payload values. */
    protected static PermanentJobException invalid(String safeMessage) {
        return new PermanentJobException(INVALID_PAYLOAD, safeMessage);
    }

    /** Parses exactly one JSON object, else {@value #INVALID_PAYLOAD}. */
    protected final JsonNode readObject(String json) {
        if (json == null || json.isBlank()) {
            throw invalid("payload is missing");
        }
        JsonNode node;
        try {
            node = strictReader.readTree(json);
        } catch (IOException e) {
            throw invalid("payload is not valid JSON");
        }
        if (node == null || !node.isObject()) {
            throw invalid("payload must be a JSON object");
        }
        return node;
    }

    /** A required non-blank string field. */
    protected static String requiredText(JsonNode object, String field) {
        String value = optionalText(object, field);
        if (value == null) {
            throw invalid("field '" + field + "' is required");
        }
        return value;
    }

    /** An optional string field: null when absent or JSON null; blank or non-string is invalid. */
    protected static String optionalText(JsonNode object, String field) {
        JsonNode node = object.get(field);
        if (node == null || node.isNull()) {
            return null;
        }
        if (!node.isTextual() || node.textValue().isBlank()) {
            throw invalid("field '" + field + "' must be a non-blank string");
        }
        return node.textValue();
    }

    /** True if {@code s} is non-empty and only visible ASCII ({@code 0x21}-{@code 0x7E}). */
    protected static boolean isVisibleAscii(String s) {
        if (s == null || s.isEmpty()) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < 0x21 || c > 0x7E) {
                return false;
            }
        }
        return true;
    }

    /** Serialises an effect payload object. */
    protected final String toJson(ObjectNode node) {
        try {
            return mapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("cannot serialise effect payload", e);
        }
    }

    /** A new empty JSON object node. */
    protected final ObjectNode newObject() {
        return mapper.createObjectNode();
    }
}
