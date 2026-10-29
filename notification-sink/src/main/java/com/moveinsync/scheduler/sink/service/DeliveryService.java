package com.moveinsync.scheduler.sink.service;

import com.moveinsync.scheduler.sink.domain.DeliveryCommand;
import com.moveinsync.scheduler.sink.domain.DeliveryResult;
import com.moveinsync.scheduler.sink.domain.Hashing;
import com.moveinsync.scheduler.sink.service.SinkMetrics.Failure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

/**
 * Entry point for one delivery request: applies injected faults (before any database access),
 * runs the fenced, idempotent transaction, and records metrics and a log line.
 *
 * <p>Logs carry only the run id, a hash of the idempotency key and the token (also as MDC fields
 * {@code runId}, {@code idemKeyHash}, {@code fencingToken}); never the payload or the raw key.
 */
@Service
public class DeliveryService {

    private static final Logger log = LoggerFactory.getLogger(DeliveryService.class);

    private final DeliveryLedger ledger;
    private final FaultInjector faults;
    private final SinkMetrics metrics;

    public DeliveryService(DeliveryLedger ledger, FaultInjector faults, SinkMetrics metrics) {
        this.ledger = ledger;
        this.faults = faults;
        this.metrics = metrics;
    }

    /**
     * @throws InjectedFaultException when fault injection fails the request; nothing was written
     */
    public DeliveryResult deliver(DeliveryCommand cmd) {
        long start = System.nanoTime();
        String keyHash = Hashing.logHash(cmd.idemKey());
        MDC.put("runId", Long.toString(cmd.runId()));
        MDC.put("idemKeyHash", keyHash);
        MDC.put("fencingToken", cmd.token().toHeader());
        try {
            try {
                faults.apply();
            } catch (InjectedFaultException e) {
                metrics.record(Failure.INJECTED_FAULT, System.nanoTime() - start);
                log.info("delivery outcome=INJECTED_FAULT status={} runId={} idemKeyHash={} fencingToken={}",
                        e.status(), cmd.runId(), keyHash, cmd.token());
                throw e;
            }
            DeliveryResult result;
            try {
                result = ledger.deliver(cmd);
            } catch (RuntimeException e) {
                metrics.record(Failure.ERROR, System.nanoTime() - start);
                throw e;
            }
            metrics.record(result.outcome(), System.nanoTime() - start);
            log.info("delivery outcome={} runId={} idemKeyHash={} fencingToken={}",
                    result.outcome(), cmd.runId(), keyHash, cmd.token());
            return result;
        } finally {
            MDC.remove("runId");
            MDC.remove("idemKeyHash");
            MDC.remove("fencingToken");
        }
    }
}
