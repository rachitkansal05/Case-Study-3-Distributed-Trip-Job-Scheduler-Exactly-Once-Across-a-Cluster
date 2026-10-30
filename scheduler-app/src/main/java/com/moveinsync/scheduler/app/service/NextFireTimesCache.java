package com.moveinsync.scheduler.app.service;

import com.moveinsync.scheduler.app.repository.job.JobRow;
import com.moveinsync.scheduler.app.service.FireTimeService.FireTimeView;
import java.time.Clock;
import java.time.ZoneId;
import java.util.List;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

/**
 * Cached next fire times of a recurring job, keyed by {@code (jobId, version, count)} with a 30 s
 * TTL (docs/design/architecture.md section 14). The result is a pure function of cron, zone and version; within the
 * TTL the list may include an instant that has just passed. A separate bean so the
 * {@code @Cacheable} proxy applies.
 */
@Component
public class NextFireTimesCache {

    private final FireTimeService fireTimes;
    private final Clock clock;

    public NextFireTimesCache(FireTimeService fireTimes, Clock clock) {
        this.fireTimes = fireTimes;
        this.clock = clock;
    }

    @Cacheable(cacheNames = CacheConfig.NEXT_FIRE_TIMES, key = "{#job.id(), #job.version(), #count}")
    public List<FireTimeView> nextFireTimes(JobRow job, ZoneId zone, int count) {
        return List.copyOf(fireTimes.nextFireTimes(job.cron(), zone, clock.instant(), count));
    }
}
