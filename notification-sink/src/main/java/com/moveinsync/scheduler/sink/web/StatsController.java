package com.moveinsync.scheduler.sink.web;

import com.moveinsync.scheduler.sink.domain.RunStats;
import com.moveinsync.scheduler.sink.domain.TotalStats;
import com.moveinsync.scheduler.sink.service.StatsService;
import java.util.List;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Read APIs for tests, the demo, and the scheduler's startup epoch guard. */
@RestController
public class StatsController {

    private final StatsService stats;

    public StatsController(StatsService stats) {
        this.stats = stats;
    }

    /** {@code GET /api/v1/deliveries/stats?runId=}: counters of one run (zeros for an unknown run). */
    @GetMapping(path = "/api/v1/deliveries/stats", params = "runId", produces = MediaType.APPLICATION_JSON_VALUE)
    public RunStats runStats(@RequestParam long runId) {
        if (runId < 1) {
            throw SinkProblemException.validation(List.of(new SinkProblemException.FieldViolation("runId",
                    "must be >= 1")));
        }
        return stats.forRun(runId);
    }

    /** {@code GET /api/v1/deliveries/stats}: totals, including {@code maxDeliveriesPerKey}. */
    @GetMapping(path = "/api/v1/deliveries/stats", params = "!runId", produces = MediaType.APPLICATION_JSON_VALUE)
    public TotalStats totals() {
        return stats.totals();
    }

    /** {@code GET /api/v1/fence/highest-epoch}: highest epoch ever accepted, 0 if none. */
    @GetMapping(path = "/api/v1/fence/highest-epoch", produces = MediaType.APPLICATION_JSON_VALUE)
    public HighestEpochResponse highestEpoch() {
        return new HighestEpochResponse(stats.highestAcceptedEpoch());
    }

    /** @param highestEpoch highest {@code max_epoch} in {@code sink.run_fence}, 0 if none */
    public record HighestEpochResponse(long highestEpoch) {
    }
}
