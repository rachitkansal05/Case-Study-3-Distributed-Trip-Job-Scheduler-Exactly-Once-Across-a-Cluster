package com.moveinsync.scheduler.app.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.moveinsync.scheduler.core.domain.HandlerType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * "Cab assigned" alerts: one SMS per employee, chunked with checkpoints.
 *
 * <p>Job payload:
 *
 * <pre>{@code
 * {"employeeIds": ["e1", "e2"],
 *  "messageTemplate": "Your cab arrives at {pickupTime}",
 *  "pickupTime": "08:05"}            // optional unless the template uses {pickupTime}
 * }</pre>
 *
 * <p>Effect payload per employee: {@code {"employeeId": "e1", "text": "<rendered template>"}};
 * {@code {employeeId}} and {@code {pickupTime}} are replaced. Item key: {@code {runIdemKey}:{employeeId}}.
 */
public final class PickupAlertHandler extends ChunkedJobHandler<PickupAlertHandler.Payload> {

    private static final String EMPLOYEE_PLACEHOLDER = "{employeeId}";
    private static final String PICKUP_PLACEHOLDER = "{pickupTime}";

    /**
     * Validated payload.
     *
     * @param employeeIds distinct, visible-ASCII ids, non-empty
     * @param messageTemplate SMS text template
     * @param pickupTime optional pickup time text
     */
    public record Payload(List<String> employeeIds, String messageTemplate, String pickupTime) {
        public Payload {
            employeeIds = List.copyOf(employeeIds);
        }
    }

    /** @param mapper shared Jackson mapper */
    public PickupAlertHandler(ObjectMapper mapper) {
        super(mapper);
    }

    /**
     * @param mapper shared Jackson mapper
     * @param chunkSize items per chunk (tests)
     */
    public PickupAlertHandler(ObjectMapper mapper, int chunkSize) {
        super(mapper, chunkSize);
    }

    @Override
    public HandlerType type() {
        return HandlerType.PICKUP_ALERT;
    }

    @Override
    protected Payload parsePayload(String payloadJson) {
        JsonNode root = readObject(payloadJson);
        JsonNode ids = root.get("employeeIds");
        if (ids == null || !ids.isArray()) {
            throw invalid("field 'employeeIds' must be an array");
        }
        if (ids.isEmpty()) {
            throw invalid("field 'employeeIds' must not be empty");
        }
        List<String> employeeIds = new ArrayList<>(ids.size());
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < ids.size(); i++) {
            JsonNode id = ids.get(i);
            if (!id.isTextual() || id.textValue().isBlank()) {
                throw invalid("employeeIds[" + i + "] must be a non-blank string");
            }
            if (!isVisibleAscii(id.textValue())) {
                throw invalid("employeeIds[" + i + "] must be visible ASCII");
            }
            if (!seen.add(id.textValue())) {
                throw invalid("employeeIds[" + i + "] is a duplicate");
            }
            employeeIds.add(id.textValue());
        }
        String template = requiredText(root, "messageTemplate");
        String pickupTime = optionalText(root, "pickupTime");
        if (pickupTime == null && template.contains(PICKUP_PLACEHOLDER)) {
            throw invalid("field 'pickupTime' is required by messageTemplate");
        }
        return new Payload(employeeIds, template, pickupTime);
    }

    @Override
    protected List<Item> items(Payload payload) {
        List<Item> items = new ArrayList<>(payload.employeeIds().size());
        for (String employeeId : payload.employeeIds()) {
            ObjectNode effect = newObject()
                    .put("employeeId", employeeId)
                    .put("text", render(payload, employeeId));
            items.add(new Item(employeeId, toJson(effect)));
        }
        return items;
    }

    private static String render(Payload payload, String employeeId) {
        String text = payload.messageTemplate().replace(EMPLOYEE_PLACEHOLDER, employeeId);
        return payload.pickupTime() == null ? text : text.replace(PICKUP_PLACEHOLDER, payload.pickupTime());
    }
}
