package com.moveinsync.scheduler.app.api;

import com.moveinsync.scheduler.app.api.dto.CreateJobRequest;
import com.moveinsync.scheduler.app.api.dto.DisableJobView;
import com.moveinsync.scheduler.app.api.dto.JobView;
import com.moveinsync.scheduler.app.api.dto.NextFireTimesView;
import com.moveinsync.scheduler.app.security.Caller;
import com.moveinsync.scheduler.app.service.IdempotencyService;
import com.moveinsync.scheduler.app.service.JobService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.net.URI;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Job definitions. Roles are enforced in {@code JwtSecurityConfig}; the tenant scope comes from
 * the {@link Caller} (token), and another tenant's job is a 404.
 */
@RestController
@RequestMapping("/api/v1/jobs")
public class JobController {

    private final JobService jobs;
    private final IdempotencyService idempotency;

    public JobController(JobService jobs, IdempotencyService idempotency) {
        this.jobs = jobs;
        this.idempotency = idempotency;
    }

    /**
     * {@code POST /api/v1/jobs} (ADMIN): 201 with the job; 400 on validation errors. With an
     * {@code Idempotency-Key}, a retry returns the original response and creates no second job.
     */
    @PostMapping
    public ResponseEntity<?> create(Caller caller, @Valid @RequestBody CreateJobRequest request,
                                    @RequestHeader(value = IdempotencyKeys.HEADER, required = false) String key) {
        return idempotency.execute(caller, IdempotencyKeys.optional(key), "POST /api/v1/jobs", request, () -> {
            JobView job = jobs.create(caller, request);
            return ResponseEntity.created(URI.create("/api/v1/jobs/" + job.id())).body(job);
        });
    }

    /** Payload shown to ADMIN only. */
    @GetMapping("/{jobId}")
    public JobView get(Caller caller, @PathVariable UUID jobId) {
        return jobs.get(caller, jobId);
    }

    @GetMapping("/{jobId}/next-fire-times")
    public NextFireTimesView nextFireTimes(Caller caller, @PathVariable UUID jobId,
                                           @RequestParam(defaultValue = "10") @Min(1) @Max(100) int count) {
        return jobs.nextFireTimes(caller, jobId, count);
    }

    /** Disables the job and cancels its unstarted runs. */
    @DeleteMapping("/{jobId}")
    public DisableJobView disable(Caller caller, @PathVariable UUID jobId) {
        return jobs.disable(caller, jobId);
    }
}
