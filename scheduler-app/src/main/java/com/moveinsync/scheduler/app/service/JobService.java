package com.moveinsync.scheduler.app.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moveinsync.scheduler.app.api.dto.CreateJobRequest;
import com.moveinsync.scheduler.app.api.dto.DisableJobView;
import com.moveinsync.scheduler.app.api.dto.JobView;
import com.moveinsync.scheduler.app.api.dto.NextFireTimesView;
import com.moveinsync.scheduler.app.config.SchedulerProperties;
import com.moveinsync.scheduler.app.error.ErrorCodes;
import com.moveinsync.scheduler.app.error.NotFoundException;
import com.moveinsync.scheduler.app.error.ValidationException;
import com.moveinsync.scheduler.app.error.ValidationException.FieldIssue;
import com.moveinsync.scheduler.app.repository.job.JobRepository;
import com.moveinsync.scheduler.app.repository.job.JobRow;
import com.moveinsync.scheduler.app.repository.job.JobRunRepository;
import com.moveinsync.scheduler.app.security.Caller;
import com.moveinsync.scheduler.app.security.TenantScope;
import com.moveinsync.scheduler.app.service.FireTimeService.FireTimeView;
import com.moveinsync.scheduler.core.domain.JobKind;
import com.moveinsync.scheduler.core.domain.MisfirePolicyType;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Job definitions: create, read, next fire times and disable (PLAN.md sections 08 and 13).
 *
 * <p>Creating a job stores only the definition, with {@code next_fire_at} set (one-off:
 * {@code run_at}; recurring: the first fire time after now) and version 1. Runs are created by
 * the Engine's materialiser from that cursor; the API never materialises occurrences.
 */
@Service
public class JobService {

    static final Duration MIN_RUN_TIME = Duration.ofSeconds(1);
    static final Duration MAX_RUN_TIME = Duration.ofHours(24);
    static final Duration MAX_MISFIRE_THRESHOLD = Duration.ofDays(7);
    static final int MAX_PAYLOAD_BYTES = 64 * 1024;
    static final int DEFAULT_PRIORITY = 5;
    static final long INITIAL_VERSION = 1;

    private static final Logger log = LoggerFactory.getLogger(JobService.class);

    private final JobRepository jobs;
    private final JobRunRepository runs;
    private final TenantSettingsService settings;
    private final FireTimeService fireTimes;
    private final NextFireTimesCache nextFireTimesCache;
    private final PartitionPlacement placement;
    private final SchedulerProperties properties;
    private final ObjectMapper mapper;
    private final Clock clock;

    public JobService(JobRepository jobs, JobRunRepository runs, TenantSettingsService settings,
                      FireTimeService fireTimes, NextFireTimesCache nextFireTimesCache,
                      PartitionPlacement placement, SchedulerProperties properties,
                      ObjectMapper mapper, Clock clock) {
        this.jobs = jobs;
        this.runs = runs;
        this.settings = settings;
        this.fireTimes = fireTimes;
        this.nextFireTimesCache = nextFireTimesCache;
        this.placement = placement;
        this.properties = properties;
        this.mapper = mapper;
        this.clock = clock;
    }

    /** Creates a job in the caller's own tenant (from the token, never from the body). */
    @Transactional
    public JobView create(Caller caller, CreateJobRequest request) {
        TenantScope scope = TenantScope.of(caller.tenantId());
        String payloadJson = validate(request);
        TenantSettings tenant = settings.settings(caller.tenantId());

        Instant nextFireAt;
        if (request.kind() == JobKind.ONE_OFF) {
            nextFireAt = request.runAt();
        } else {
            List<FireTimeView> first = fireTimes.nextFireTimes(request.cron(), tenant.zone(), clock.instant(), 1);
            if (first.isEmpty()) {
                throw ValidationException.invalidCron("has no future fire time");
            }
            nextFireAt = first.get(0).instant();
        }

        UUID id = UUID.randomUUID();
        JobRow row = new JobRow(id, caller.tenantId(), request.kind(), request.handler(), request.cron(),
                request.runAt(), payloadJson,
                request.priority() == null ? DEFAULT_PRIORITY : request.priority(),
                request.maxRunTime(),
                request.misfire() == null ? MisfirePolicyType.FIRE_NOW : request.misfire(),
                request.misfireThreshold() == null ? properties.misfire().defaultThreshold() : request.misfireThreshold(),
                request.maxAttempts() == null ? properties.retry().maxAttempts() : request.maxAttempts(),
                placement.partitionFor(id), nextFireAt, INITIAL_VERSION, true);
        jobs.insert(scope, row);
        log.info("job {} created: kind={} handler={} partition={}", id, row.kind(), row.handler(), row.partitionId());
        return view(row, caller);
    }

    @Transactional(readOnly = true)
    public JobView get(Caller caller, UUID jobId) {
        return view(load(caller.scope(), jobId), caller);
    }

    /**
     * Next {@code count} fire times. A disabled job has none; a one-off job has its {@code run_at};
     * recurring results come from {@link NextFireTimesCache} (30 s TTL).
     */
    @Transactional(readOnly = true)
    public NextFireTimesView nextFireTimes(Caller caller, UUID jobId, int count) {
        JobRow job = load(caller.scope(), jobId);
        TenantSettings tenant = settings.settings(job.tenantId());
        List<FireTimeView> times;
        if (!job.enabled()) {
            times = List.of();
        } else if (job.kind() == JobKind.ONE_OFF) {
            times = List.of(FireTimeView.of(job.runAt(), tenant.zone()));
        } else {
            times = nextFireTimesCache.nextFireTimes(job, tenant.zone(), count);
        }
        return new NextFireTimesView(job.id(), tenant.zone().getId(), times);
    }

    /** Disables the job and cancels its unstarted runs in one transaction. Idempotent. */
    @Transactional
    public DisableJobView disable(Caller caller, UUID jobId) {
        TenantScope scope = caller.scope();
        if (!jobs.disable(scope, jobId)) {
            throw NotFoundException.job();
        }
        int cancelled = runs.cancelUnstarted(scope, jobId);
        log.info("job {} disabled; {} unstarted runs cancelled", jobId, cancelled);
        return new DisableJobView(jobId, false, cancelled);
    }

    JobRow load(TenantScope scope, UUID jobId) {
        return jobs.find(scope, jobId).orElseThrow(NotFoundException::job);
    }

    // ------------------------------------------------------------------ validation

    /** Checks the kind rules, bounds and payload; returns the payload as JSON text. */
    private String validate(CreateJobRequest r) {
        List<FieldIssue> issues = new ArrayList<>();
        if (r.kind() == JobKind.ONE_OFF) {
            if (r.runAt() == null) {
                issues.add(new FieldIssue("runAt", "is required for ONE_OFF jobs"));
            }
            if (r.cron() != null) {
                issues.add(new FieldIssue("cron", "must be absent for ONE_OFF jobs"));
            }
        } else {
            if (r.cron() == null || r.cron().isBlank()) {
                issues.add(new FieldIssue("cron", "is required for RECURRING jobs"));
            }
            if (r.runAt() != null) {
                issues.add(new FieldIssue("runAt", "must be absent for RECURRING jobs"));
            }
        }
        if (r.maxRunTime().compareTo(MIN_RUN_TIME) < 0 || r.maxRunTime().compareTo(MAX_RUN_TIME) > 0) {
            issues.add(new FieldIssue("maxRunTime", "must be between PT1S and PT24H"));
        }
        if (r.misfireThreshold() != null
                && (r.misfireThreshold().isNegative() || r.misfireThreshold().compareTo(MAX_MISFIRE_THRESHOLD) > 0)) {
            issues.add(new FieldIssue("misfireThreshold", "must be between PT0S and P7D"));
        }
        String payloadJson = payloadJson(r.payload(), issues);
        if (!issues.isEmpty()) {
            throw new ValidationException(ErrorCodes.VALIDATION_FAILED, "Request validation failed", issues);
        }
        if (r.kind() == JobKind.RECURRING) {
            validateCron(r.cron());
        }
        return payloadJson;
    }

    private void validateCron(String cron) {
        try {
            fireTimes.validateCron(cron);
        } catch (IllegalArgumentException e) {
            throw ValidationException.invalidCron("is not a valid cron expression");
        }
    }

    private String payloadJson(JsonNode payload, List<FieldIssue> issues) {
        if (!payload.isObject()) {
            issues.add(new FieldIssue("payload", "must be a JSON object"));
            return null;
        }
        try {
            String json = mapper.writeValueAsString(payload);
            if (json.getBytes(StandardCharsets.UTF_8).length > MAX_PAYLOAD_BYTES) {
                issues.add(new FieldIssue("payload", "must be at most 64 KiB"));
            }
            return json;
        } catch (JsonProcessingException e) {
            issues.add(new FieldIssue("payload", "must be valid JSON"));
            return null;
        }
    }

    // ------------------------------------------------------------------ views

    private JobView view(JobRow j, Caller caller) {
        boolean showPayload = caller.canSeePayload();
        return new JobView(j.id(), j.tenantId(), j.kind(), j.handler(), j.cron(), j.runAt(),
                showPayload ? readPayload(j.payloadJson()) : null, !showPayload,
                j.priority(), j.maxRunTime(), j.misfire(), j.misfireThreshold(), j.maxAttempts(), j.partitionId(),
                j.nextFireAt(), j.version(), j.enabled());
    }

    private JsonNode readPayload(String json) {
        try {
            return mapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("stored payload is not valid JSON");
        }
    }
}
