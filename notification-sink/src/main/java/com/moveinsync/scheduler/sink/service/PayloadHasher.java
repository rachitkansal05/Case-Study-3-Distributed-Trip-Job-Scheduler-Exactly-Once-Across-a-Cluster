package com.moveinsync.scheduler.sink.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.sink.domain.Hashing;
import org.springframework.stereotype.Component;

/**
 * SHA-256 of a delivery payload. The payload may contain phone numbers, e-mails and home
 * locations, so only this hash is ever stored. The hash is taken over the payload re-serialised
 * compactly (field order as received, no insignificant whitespace).
 */
@Component
public class PayloadHasher {

    private final ObjectMapper objectMapper;

    public PayloadHasher(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String sha256Hex(JsonNode payload) {
        try {
            return Hashing.sha256Hex(objectMapper.writeValueAsBytes(payload));
        } catch (JsonProcessingException e) {
            // Never include the payload or Jackson's message (which may quote it).
            throw new IllegalStateException("payload could not be serialised for hashing");
        }
    }
}
