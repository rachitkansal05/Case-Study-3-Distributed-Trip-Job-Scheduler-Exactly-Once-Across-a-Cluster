package com.moveinsync.scheduler.app.service;

import com.moveinsync.scheduler.app.api.dto.DeadLetterView;
import com.moveinsync.scheduler.app.api.dto.PageView;
import com.moveinsync.scheduler.app.api.dto.ReplayView;
import com.moveinsync.scheduler.app.error.ConflictException;
import com.moveinsync.scheduler.app.error.ErrorCodes;
import com.moveinsync.scheduler.app.error.NotFoundException;
import com.moveinsync.scheduler.app.repository.deadletter.DeadLetterRepository;
import com.moveinsync.scheduler.app.repository.deadletter.DeadLetterRepository.DeadLetterRow;
import com.moveinsync.scheduler.app.security.Caller;
import com.moveinsync.scheduler.app.security.TenantScope;
import com.moveinsync.scheduler.core.domain.RunState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** The dead-letter list and operator replay (docs/design/architecture.md section 09). */
@Service
public class DeadLetterService {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterService.class);

    private final DeadLetterRepository deadLetters;

    public DeadLetterService(DeadLetterRepository deadLetters) {
        this.deadLetters = deadLetters;
    }

    /**
     * Dead letters of {@code tenant}, or of every tenant the caller may see when it is null
     * (OPERATOR: all; ADMIN: their own). Naming another tenant is a 404.
     */
    @Transactional(readOnly = true)
    public PageView<DeadLetterView> list(Caller caller, String tenant, int page, int size) {
        TenantScope scope = tenant == null ? caller.scope() : caller.scope().narrowTo(tenant);
        var rows = deadLetters.page(scope, page * size, size + 1).stream().map(DeadLetterService::view).toList();
        return PageView.of(rows, page, size);
    }

    /**
     * Replays a dead-lettered run: the same row goes back to {@code SCHEDULED}, due now, with
     * attempts reset, and {@code dead_letter.replayed_at} is set. The {@code idem_key} is kept.
     */
    @Transactional
    public ReplayView replay(Caller caller, long runId) {
        TenantScope scope = caller.scope();
        DeadLetterRow dl = deadLetters.lockForReplay(scope, runId).orElseThrow(NotFoundException::deadLetter);
        if (dl.runState() != RunState.DEAD_LETTERED) {
            throw new ConflictException(ErrorCodes.DEAD_LETTER_ALREADY_REPLAYED,
                    "Run is no longer dead-lettered; it was already replayed");
        }
        if (!dl.jobEnabled()) {
            throw new ConflictException(ErrorCodes.JOB_DISABLED, "Job is disabled");
        }
        deadLetters.rescheduleRun(scope, runId);
        deadLetters.markReplayed(scope, runId);
        DeadLetterRow after = deadLetters.lockForReplay(scope, runId).orElseThrow(NotFoundException::deadLetter);
        log.info("dead-lettered run {} of job {} replayed", runId, dl.jobId());
        return new ReplayView(runId, after.jobId(), after.runState(), after.dueAt(), after.idemKey(),
                after.replayedAt());
    }

    private static DeadLetterView view(DeadLetterRow r) {
        return new DeadLetterView(r.runId(), r.jobId(), r.tenantId(), r.errorCode(), r.attempts(), r.deadAt(),
                r.replayedAt(), r.runState());
    }
}
