package com.moveinsync.scheduler.sink.web;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.cfg.CoercionAction;
import com.fasterxml.jackson.databind.cfg.CoercionInputShape;
import com.fasterxml.jackson.databind.type.LogicalType;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.moveinsync.scheduler.core.domain.DeliveryOutcome;
import com.moveinsync.scheduler.core.domain.FencingToken;
import com.moveinsync.scheduler.sink.domain.DeliveryCommand;
import com.moveinsync.scheduler.sink.domain.DeliveryResult;
import com.moveinsync.scheduler.sink.service.DeliveryService;
import com.moveinsync.scheduler.sink.service.PayloadHasher;
import com.moveinsync.scheduler.sink.web.SinkProblemException.FieldViolation;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** {@code POST /api/v1/deliveries}: the fenced, idempotent effect endpoint. See {@code docs/sink-api.md}. */
@RestController
@RequestMapping("/api/v1/deliveries")
public class DeliveryController {

    private final DeliveryService deliveries;
    private final PayloadHasher hasher;
    private final ObjectReader requestReader;

    public DeliveryController(DeliveryService deliveries, PayloadHasher hasher, ObjectMapper objectMapper) {
        this.deliveries = deliveries;
        this.hasher = hasher;
        this.requestReader = strict(objectMapper).readerFor(DeliveryRequest.class);
    }

    /** No scalar coercion: {@code "runId":"123"} or {@code "runId":1.5} is a type error, not 123 or 1. */
    private static ObjectMapper strict(ObjectMapper base) {
        ObjectMapper strict = base.copy();
        strict.coercionConfigFor(LogicalType.Integer)
                .setCoercion(CoercionInputShape.String, CoercionAction.Fail)
                .setCoercion(CoercionInputShape.Float, CoercionAction.Fail)
                .setCoercion(CoercionInputShape.Boolean, CoercionAction.Fail);
        strict.coercionConfigFor(LogicalType.Textual)
                .setCoercion(CoercionInputShape.Integer, CoercionAction.Fail)
                .setCoercion(CoercionInputShape.Float, CoercionAction.Fail)
                .setCoercion(CoercionInputShape.Boolean, CoercionAction.Fail);
        return strict;
    }

    /**
     * 201 {@code DELIVERED}, 200 {@code ALREADY_DELIVERED}, or 409 {@code STALE_TOKEN} (ProblemDetail).
     * Validation order is fixed: {@code Idempotency-Key}, then {@code Fencing-Token}, then the body
     * (which is parsed here rather than by Spring so that the order holds and parse errors never
     * echo request content).
     */
    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<DeliveryResponse> deliver(
            @RequestHeader(name = RequestHeaders.IDEMPOTENCY_KEY, required = false) String idempotencyKey,
            @RequestHeader(name = FencingToken.HEADER, required = false) String fencingToken,
            @RequestBody(required = false) byte[] rawBody) {
        String key = RequestHeaders.idempotencyKey(idempotencyKey);
        FencingToken token = RequestHeaders.fencingToken(fencingToken);
        DeliveryRequest body = parse(rawBody);
        List<FieldViolation> violations = body.violations();
        if (!violations.isEmpty()) {
            throw SinkProblemException.validation(violations);
        }
        DeliveryCommand cmd = new DeliveryCommand(body.runId(), key, token, body.channelValue(),
                hasher.sha256Hex(body.payload()));

        DeliveryResult result = deliveries.deliver(cmd);
        if (result.outcome() == DeliveryOutcome.STALE_TOKEN) {
            throw new SinkProblemException(ProblemCode.STALE_TOKEN,
                    "A higher fencing token has already been accepted for this run; nothing was delivered.");
        }
        HttpStatus status = result.outcome() == DeliveryOutcome.DELIVERED ? HttpStatus.CREATED : HttpStatus.OK;
        return ResponseEntity.status(status).body(DeliveryResponse.of(result));
    }

    private DeliveryRequest parse(byte[] rawBody) {
        if (rawBody == null || rawBody.length == 0) {
            throw new SinkProblemException(ProblemCode.MALFORMED_BODY, "A JSON request body is required.");
        }
        try {
            DeliveryRequest body = requestReader.readValue(rawBody);
            if (body == null) {
                throw new SinkProblemException(ProblemCode.MALFORMED_BODY, "The request body must be a JSON object.");
            }
            return body;
        } catch (MismatchedInputException e) {
            String field = e.getPath().stream()
                    .map(ref -> ref.getFieldName() != null ? ref.getFieldName() : "[" + ref.getIndex() + "]")
                    .collect(Collectors.joining("."));
            if (field.isEmpty()) {
                throw new SinkProblemException(ProblemCode.MALFORMED_BODY, "The request body must be a JSON object.");
            }
            throw SinkProblemException.validation(List.of(new FieldViolation(field, "has the wrong type")));
        } catch (JsonProcessingException e) {
            throw new SinkProblemException(ProblemCode.MALFORMED_BODY, "The request body is not valid JSON.");
        } catch (IOException e) {
            throw new SinkProblemException(ProblemCode.MALFORMED_BODY, "The request body could not be read.");
        }
    }
}
