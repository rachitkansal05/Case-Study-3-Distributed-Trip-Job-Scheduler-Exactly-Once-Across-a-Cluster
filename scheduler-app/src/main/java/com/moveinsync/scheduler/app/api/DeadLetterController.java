package com.moveinsync.scheduler.app.api;

import com.moveinsync.scheduler.app.api.dto.DeadLetterView;
import com.moveinsync.scheduler.app.api.dto.PageView;
import com.moveinsync.scheduler.app.api.dto.ReplayView;
import com.moveinsync.scheduler.app.security.Caller;
import com.moveinsync.scheduler.app.service.DeadLetterService;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Dead-letter list (OPERATOR, ADMIN own tenant) and replay (OPERATOR). */
@RestController
@RequestMapping("/api/v1/dead-letters")
public class DeadLetterController {

    private final DeadLetterService deadLetters;

    public DeadLetterController(DeadLetterService deadLetters) {
        this.deadLetters = deadLetters;
    }

    /** {@code ?tenant=} optional: OPERATOR sees all tenants without it, ADMIN their own. */
    @GetMapping
    public PageView<DeadLetterView> list(Caller caller,
                                         @RequestParam(required = false) String tenant,
                                         @RequestParam(defaultValue = "0") @Min(0) @Max(10_000) int page,
                                         @RequestParam(defaultValue = "50") @Min(1) @Max(200) int size) {
        return deadLetters.list(caller, tenant, page, size);
    }

    @PostMapping("/{runId}/replay")
    public ReplayView replay(Caller caller, @PathVariable long runId) {
        return deadLetters.replay(caller, runId);
    }
}
