package com.moveinsync.scheduler.app.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.core.policy.PermanentJobException;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Handler that sends one effect per item, in chunks, with a checkpoint {@code {"nextItem":n}}
 * after every chunk (plan section 07). Item {@code i} uses the key {@code {runIdemKey}:{itemId}},
 * so a replay after a crash re-sends at most one chunk and the sink answers {@code
 * ALREADY_DELIVERED} for those.
 *
 * <p>The item list must be deterministic for a given payload: the checkpoint is an index into it.
 *
 * @param <P> validated payload type
 */
public abstract class ChunkedJobHandler<P> extends JobHandler<P> {

    /** Default items per chunk. */
    public static final int CHUNK_SIZE = 50;

    /** Error code for a checkpoint that cannot be read. */
    public static final String INVALID_CHECKPOINT = "INVALID_CHECKPOINT";

    /** The sink limits {@code Idempotency-Key} to this many characters. */
    static final int MAX_KEY_LENGTH = 256;

    /**
     * One effect of a chunked run.
     *
     * @param itemId stable id, visible ASCII; becomes the suffix of the item key
     * @param effectPayloadJson JSON object sent as the sink payload
     */
    public record Item(String itemId, String effectPayloadJson) {
        public Item {
            Objects.requireNonNull(itemId, "itemId");
            Objects.requireNonNull(effectPayloadJson, "effectPayloadJson");
        }
    }

    private final int chunkSize;

    /** @param mapper shared Jackson mapper; chunks of {@value #CHUNK_SIZE} */
    protected ChunkedJobHandler(ObjectMapper mapper) {
        this(mapper, CHUNK_SIZE);
    }

    /**
     * @param mapper shared Jackson mapper
     * @param chunkSize items per chunk (tests)
     */
    protected ChunkedJobHandler(ObjectMapper mapper, int chunkSize) {
        super(mapper);
        if (chunkSize < 1) {
            throw new IllegalArgumentException("chunkSize must be >= 1");
        }
        this.chunkSize = chunkSize;
    }

    /** Items per chunk. */
    public final int chunkSize() {
        return chunkSize;
    }

    /** All items of the payload, in a deterministic order. */
    protected abstract List<Item> items(P payload);

    @Override
    protected final void execute(EffectContext ctx, P payload) throws InterruptedException {
        List<Item> items = List.copyOf(items(payload));
        // Validate every key before the first send, so a bad item never leaves a half-sent run.
        List<String> keys = items.stream().map(item -> itemKey(ctx, item.itemId())).toList();
        int next = nextItem(ctx.run().checkpointJson(), items.size());
        while (next < items.size()) {
            int end = Math.min(next + chunkSize, items.size());
            for (int i = next; i < end; i++) {
                if (Thread.interrupted()) {
                    throw new InterruptedException("interrupted between items");
                }
                ctx.send(keys.get(i), items.get(i).effectPayloadJson());
            }
            next = end;
            ctx.checkpoint("{\"nextItem\":" + next + "}");
        }
    }

    private static String itemKey(EffectContext ctx, String itemId) {
        if (!isVisibleAscii(itemId)) {
            throw invalid("item id must be visible ASCII");
        }
        String key = ctx.run().itemKey(itemId);
        if (key.length() > MAX_KEY_LENGTH) {
            throw invalid("item key longer than " + MAX_KEY_LENGTH + " characters");
        }
        return key;
    }

    /** Index to resume from: 0 without a checkpoint, else {@code nextItem} in {@code [0, size]}. */
    private int nextItem(String checkpointJson, int size) {
        if (checkpointJson == null) {
            return 0;
        }
        JsonNode next;
        try {
            JsonNode root = mapper().readTree(checkpointJson);
            next = root == null || !root.isObject() ? null : root.get("nextItem");
        } catch (IOException e) {
            next = null;
        }
        if (next == null || !next.isIntegralNumber() || !next.canConvertToInt()
                || next.intValue() < 0 || next.intValue() > size) {
            throw new PermanentJobException(INVALID_CHECKPOINT, "checkpoint must be {\"nextItem\": 0.." + size + "}");
        }
        return next.intValue();
    }
}
