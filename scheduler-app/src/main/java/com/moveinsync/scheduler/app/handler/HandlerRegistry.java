package com.moveinsync.scheduler.app.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.core.domain.HandlerType;
import com.moveinsync.scheduler.core.policy.PermanentJobException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Looks up the {@link JobHandler} of a {@link HandlerType}. Immutable after construction. */
public final class HandlerRegistry {

    /** Error code when no handler is registered for a job's type. */
    public static final String NO_HANDLER = "NO_HANDLER";

    private final Map<HandlerType, JobHandler<?>> handlers = new EnumMap<>(HandlerType.class);

    /**
     * @param handlers at most one per type
     * @throws IllegalArgumentException on two handlers for one type
     */
    public HandlerRegistry(Collection<? extends JobHandler<?>> handlers) {
        for (JobHandler<?> handler : Objects.requireNonNull(handlers, "handlers")) {
            HandlerType type = Objects.requireNonNull(handler, "handler").type();
            if (this.handlers.putIfAbsent(type, handler) != null) {
                throw new IllegalArgumentException("duplicate handler for " + type);
            }
        }
    }

    /** Registry with the four built-in handlers. */
    public static HandlerRegistry defaults(ObjectMapper mapper) {
        return new HandlerRegistry(List.of(
                new PickupAlertHandler(mapper),
                new RosterGenerationHandler(mapper),
                new NoShowCancelHandler(mapper),
                new BillingCloseHandler(mapper)));
    }

    /**
     * @throws PermanentJobException {@value #NO_HANDLER} if none is registered
     */
    public JobHandler<?> get(HandlerType type) {
        JobHandler<?> handler = handlers.get(Objects.requireNonNull(type, "type"));
        if (handler == null) {
            throw new PermanentJobException(NO_HANDLER, "no handler registered for " + type);
        }
        return handler;
    }

    /** Types with a registered handler. */
    public Set<HandlerType> types() {
        return Collections.unmodifiableSet(handlers.keySet());
    }
}
