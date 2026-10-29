package com.moveinsync.scheduler.app.api.support;

import com.moveinsync.scheduler.app.error.ValidationException;
import com.moveinsync.scheduler.app.service.FireTimeService;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test stand-in for Core's cron engine.
 * <ul>
 *   <li>{@link #NEVER} is valid but has no future fire time;</li>
 *   <li>any other expression with 5 or 6 space-separated fields and without the word
 *       {@code invalid} is valid and fires at the top of every hour;</li>
 *   <li>everything else is rejected with {@code INVALID_CRON}.</li>
 * </ul>
 * {@link #calls()} counts {@code nextFireTimes} calls so caching can be observed.
 */
public class FakeFireTimeService implements FireTimeService {

    /** A valid expression that never fires. */
    public static final String NEVER = "0 0 0 30 2 *";
    /** A valid hourly expression. */
    public static final String HOURLY = "0 0 * * * *";

    private final AtomicInteger calls = new AtomicInteger();

    @Override
    public void validateCron(String cron) {
        String[] fields = cron.trim().split("\\s+");
        if (cron.contains("invalid") || fields.length < 5 || fields.length > 6) {
            throw ValidationException.invalidCron("is not a valid cron expression");
        }
    }

    @Override
    public List<FireTimeView> nextFireTimes(String cron, ZoneId zone, Instant after, int count) {
        calls.incrementAndGet();
        validateCron(cron);
        if (NEVER.equals(cron)) {
            return List.of();
        }
        List<FireTimeView> out = new ArrayList<>();
        Instant next = after.truncatedTo(ChronoUnit.HOURS).plus(1, ChronoUnit.HOURS);
        for (int i = 0; i < count; i++) {
            out.add(FireTimeView.of(next, zone));
            next = next.plus(1, ChronoUnit.HOURS);
        }
        return out;
    }

    public int calls() {
        return calls.get();
    }
}
