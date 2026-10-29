package com.moveinsync.scheduler.app.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.moveinsync.scheduler.app.api.dto.PageView;
import com.moveinsync.scheduler.app.api.dto.RunView;
import com.moveinsync.scheduler.app.security.Caller;
import com.moveinsync.scheduler.app.service.IdempotencyService;
import com.moveinsync.scheduler.app.service.RunService;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Manual trigger and run history of a job (ADMIN of the tenant, or OPERATOR). */
@RestController
@RequestMapping("/api/v1/jobs/{jobId}")
public class JobRunController {

    private final RunService runs;
    private final IdempotencyService idempotency;

    public JobRunController(RunService runs, IdempotencyService idempotency) {
        this.runs = runs;
        this.idempotency = idempotency;
    }

    /**
     * {@code POST /api/v1/jobs/{id}/trigger}: 202 with the run. The {@code Idempotency-Key}
     * header is required; repeating it returns the same run ({@code duplicate = true}).
     */
    @PostMapping("/trigger")
    public ResponseEntity<?> trigger(Caller caller, @PathVariable UUID jobId,
                                     @RequestHeader(IdempotencyKeys.HEADER) String idempotencyKey) {
        String key = IdempotencyKeys.requireValid(idempotencyKey);
        return idempotency.execute(caller, key, "POST /api/v1/jobs/" + jobId + "/trigger", null,
                () -> ResponseEntity.status(HttpStatus.ACCEPTED).body(runs.trigger(caller, jobId, key)),
                JobRunController::markDuplicate);
    }

    /** A replayed trigger returns the same run, so it is a duplicate by definition. */
    private static JsonNode markDuplicate(JsonNode body) {
        if (body instanceof ObjectNode object) {
            object.put("duplicate", true);
        }
        return body;
    }

    @GetMapping("/runs")
    public PageView<RunView> runs(Caller caller, @PathVariable UUID jobId,
                                  @RequestParam(defaultValue = "0") @Min(0) @Max(10_000) int page,
                                  @RequestParam(defaultValue = "50") @Min(1) @Max(200) int size) {
        return runs.history(caller, jobId, page, size);
    }
}
