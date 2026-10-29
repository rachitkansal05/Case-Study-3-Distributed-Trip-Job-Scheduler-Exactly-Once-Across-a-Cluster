package com.moveinsync.scheduler.app.repository.idempotency;

import java.time.Duration;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * {@code api_idempotency}: stored responses of retry-safe writes, keyed by
 * {@code (tenant_id, idem_key)}. {@code tenant_id} holds the caller's idempotency namespace (their
 * tenant, or {@code operator:<tenant>} for operators), so keys never cross tenants.
 */
@Repository
public class IdempotencyRepository {

    private final JdbcClient jdbc;

    public IdempotencyRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Serialises requests with the same key: a transaction-scoped advisory lock, released at
     * commit or rollback. A second identical request waits here, then finds the stored response.
     * The two-key form hashes namespace and key separately; a hash collision only serialises two
     * unrelated requests, it never mixes up their records.
     */
    public void lock(String namespace, String key) {
        jdbc.sql("SELECT pg_advisory_xact_lock(hashtext(:ns), hashtext(:key))")
                .param("ns", namespace)
                .param("key", key)
                .query((rs, i) -> 1)
                .single();
    }

    /** The stored record, if one was written less than {@code ttl} ago. */
    public Optional<StoredResponse> find(String namespace, String key, Duration ttl) {
        return jdbc.sql("""
                        SELECT request_hash, response::text AS response FROM api_idempotency
                         WHERE tenant_id = :ns AND idem_key = :key
                           AND created_at > clock_timestamp() - :ttlMs * interval '1 millisecond'""")
                .param("ns", namespace)
                .param("key", key)
                .param("ttlMs", ttl.toMillis())
                .query((rs, i) -> new StoredResponse(rs.getString("request_hash"), rs.getString("response")))
                .optional();
    }

    /** Stores the response, replacing an expired record with the same key. */
    public void save(String namespace, String key, String requestHash, String responseJson) {
        jdbc.sql("""
                        INSERT INTO api_idempotency (tenant_id, idem_key, request_hash, response, created_at)
                        VALUES (:ns, :key, :hash, CAST(:response AS jsonb), clock_timestamp())
                        ON CONFLICT (tenant_id, idem_key) DO UPDATE
                           SET request_hash = EXCLUDED.request_hash, response = EXCLUDED.response,
                               created_at = EXCLUDED.created_at""")
                .param("ns", namespace)
                .param("key", key)
                .param("hash", requestHash)
                .param("response", responseJson)
                .update();
    }

    /**
     * @param responseJson {@code {"status": 201, "location": "...", "body": {...}}}
     */
    public record StoredResponse(String requestHash, String responseJson) {
    }
}
