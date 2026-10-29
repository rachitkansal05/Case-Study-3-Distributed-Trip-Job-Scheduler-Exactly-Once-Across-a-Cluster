package com.moveinsync.scheduler.app.service;

import com.moveinsync.scheduler.app.api.dto.PageView;
import com.moveinsync.scheduler.app.api.dto.RunView;
import com.moveinsync.scheduler.app.api.dto.TriggerView;
import com.moveinsync.scheduler.app.error.ConflictException;
import com.moveinsync.scheduler.app.error.ErrorCodes;
import com.moveinsync.scheduler.app.error.NotFoundException;
import com.moveinsync.scheduler.app.repository.job.JobRepository;
import com.moveinsync.scheduler.app.repository.job.JobRow;
import com.moveinsync.scheduler.app.repository.job.JobRunRepository;
import com.moveinsync.scheduler.app.repository.job.RunRow;
import com.moveinsync.scheduler.app.security.Caller;
import com.moveinsync.scheduler.app.security.TenantScope;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Manual triggers and run history of a job. */
@Service
public class RunService {

    private static final Logger log = LoggerFactory.getLogger(RunService.class);

    private final JobRepository jobs;
    private final JobRunRepository runs;

    public RunService(JobRepository jobs, JobRunRepository runs) {
        this.jobs = jobs;
        this.runs = runs;
    }

    /** {@code job_run.idem_key} of a manual run: {@code <jobId>:manual:<Idempotency-Key>}. */
    public static String manualIdemKey(UUID jobId, String requestKey) {
        return jobId + ":manual:" + requestKey;
    }

    /**
     * Inserts a {@code SCHEDULED} run due now, copying priority, max run time, version and
     * partition from the job. The same {@code Idempotency-Key} always yields the same run (unique
     * {@code idem_key}), even after the 24 h API idempotency record has expired.
     *
     * <p>The job row is read {@code FOR SHARE}, so a concurrent disable either waits for this
     * transaction (and then cancels the new run) or has committed already (and this sees
     * {@code enabled = false}).
     */
    @Transactional
    public TriggerView trigger(Caller caller, UUID jobId, String requestKey) {
        TenantScope scope = caller.scope();
        JobRow job = jobs.findForShare(scope, jobId).orElseThrow(NotFoundException::job);
        String idemKey = manualIdemKey(jobId, requestKey);
        if (!job.enabled()) {
            // a retried trigger of a since-disabled job still reports the run it created
            return runs.findByIdemKey(scope, idemKey).map(r -> view(r, true))
                    .orElseThrow(() -> new ConflictException(ErrorCodes.JOB_DISABLED, "Job is disabled"));
        }
        var inserted = runs.insertManualRun(scope, job, idemKey);
        RunRow run = runs.findByIdemKey(scope, idemKey).orElseThrow(NotFoundException::job);
        if (inserted.isPresent()) {
            log.info("manual run {} of job {} scheduled", run.runId(), jobId);
        }
        return view(run, inserted.isEmpty());
    }

    /** Runs of the job, newest first. Another tenant's job is a 404 even when it has no runs. */
    @Transactional(readOnly = true)
    public PageView<RunView> history(Caller caller, UUID jobId, int page, int size) {
        TenantScope scope = caller.scope();
        jobs.find(scope, jobId).orElseThrow(NotFoundException::job);
        var rows = runs.page(scope, jobId, page * size, size + 1).stream().map(RunView::of).toList();
        return PageView.of(rows, page, size);
    }

    private static TriggerView view(RunRow run, boolean duplicate) {
        return new TriggerView(run.runId(), run.jobId(), run.state(), run.dueAt(), duplicate);
    }
}
