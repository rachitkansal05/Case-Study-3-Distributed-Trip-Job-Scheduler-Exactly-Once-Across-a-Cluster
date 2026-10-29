package com.moveinsync.scheduler.sink.web;

import com.moveinsync.scheduler.sink.domain.FaultSettings;
import com.moveinsync.scheduler.sink.service.FaultInjector;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/** {@code /admin/faults}: fault injection for chaos and benchmark tests. Needs {@code X-Admin-Token}. */
@RestController
@RequestMapping(path = "/admin/faults", produces = MediaType.APPLICATION_JSON_VALUE)
public class AdminFaultController {

    private static final Logger log = LoggerFactory.getLogger(AdminFaultController.class);

    private final FaultInjector faults;

    public AdminFaultController(FaultInjector faults) {
        this.faults = faults;
    }

    @GetMapping
    public FaultSettings current() {
        return faults.current();
    }

    /** Replaces the fault settings; returns them. */
    @PutMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    public FaultSettings set(@RequestBody FaultRequest request) {
        List<SinkProblemException.FieldViolation> violations = request.violations();
        if (!violations.isEmpty()) {
            throw SinkProblemException.validation(violations);
        }
        FaultSettings settings = request.toSettings();
        faults.set(settings);
        log.warn("fault injection set latencyMs={} latencyJitterMs={} failureRate={} failureStatus={}",
                settings.latencyMs(), settings.latencyJitterMs(), settings.failureRate(), settings.failureStatus());
        return settings;
    }

    /** Clears all faults. */
    @DeleteMapping
    public ResponseEntity<Void> clear() {
        faults.clear();
        log.info("fault injection cleared");
        return ResponseEntity.noContent().build();
    }
}
