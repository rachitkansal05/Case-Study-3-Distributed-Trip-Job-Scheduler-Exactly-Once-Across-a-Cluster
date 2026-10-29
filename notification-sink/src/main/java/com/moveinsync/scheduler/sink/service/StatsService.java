package com.moveinsync.scheduler.sink.service;

import com.moveinsync.scheduler.sink.domain.RunStats;
import com.moveinsync.scheduler.sink.domain.TotalStats;
import com.moveinsync.scheduler.sink.repository.RunFenceRepository;
import com.moveinsync.scheduler.sink.repository.StatsRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Read side: delivery counters for tests and the demo, and the epoch for the startup guard. */
@Service
public class StatsService {

    private final StatsRepository stats;
    private final RunFenceRepository fences;

    public StatsService(StatsRepository stats, RunFenceRepository fences) {
        this.stats = stats;
        this.fences = fences;
    }

    @Transactional(readOnly = true)
    public RunStats forRun(long runId) {
        return stats.forRun(runId);
    }

    @Transactional(readOnly = true)
    public TotalStats totals() {
        return stats.totals();
    }

    /** Highest {@code max_epoch} the sink has ever accepted, or 0 if none (plan F5). */
    @Transactional(readOnly = true)
    public long highestAcceptedEpoch() {
        return fences.highestEpoch();
    }
}
