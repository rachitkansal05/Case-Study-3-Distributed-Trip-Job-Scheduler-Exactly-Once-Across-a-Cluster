package com.moveinsync.scheduler.app.service;

import com.moveinsync.scheduler.app.error.ValidationException;
import com.moveinsync.scheduler.core.time.CronSchedule;
import com.moveinsync.scheduler.core.time.InvalidCronException;
import com.moveinsync.scheduler.core.time.NextFireCalculator;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import org.springframework.stereotype.Component;

/**
 * {@link FireTimeService} backed by Core's {@link CronSchedule} and {@link NextFireCalculator}, so
 * the API and the engine's materialiser apply the same DST rules (docs/design/architecture.md section 08).
 */
@Component
public class CoreFireTimeService implements FireTimeService {

    private final NextFireCalculator calculator = new NextFireCalculator();

    @Override
    public void validateCron(String cron) {
        parse(cron);
    }

    @Override
    public List<FireTimeView> nextFireTimes(String cron, ZoneId zone, Instant after, int count) {
        return calculator.next(parse(cron), zone, after, count).stream()
                .map(f -> new FireTimeView(f.instant(), f.localTime(), f.offset()))
                .toList();
    }

    private static CronSchedule parse(String cron) {
        try {
            return CronSchedule.parse(cron);
        } catch (InvalidCronException e) {
            // Core's messages describe the field and rule, never echo the whole expression.
            throw ValidationException.invalidCron(e.getMessage());
        }
    }
}
