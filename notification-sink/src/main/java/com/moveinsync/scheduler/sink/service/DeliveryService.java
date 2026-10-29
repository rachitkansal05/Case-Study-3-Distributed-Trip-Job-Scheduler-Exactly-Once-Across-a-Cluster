package com.moveinsync.scheduler.sink.service;

import com.moveinsync.scheduler.sink.domain.DeliveryCommand;
import com.moveinsync.scheduler.sink.domain.DeliveryResult;
import com.moveinsync.scheduler.sink.domain.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Entry point for one delivery request: applies injected faults (before any database access),
 * runs the fenced, idempotent transaction and logs the verdict. Logs carry the run id, a hash of
 * the idempotency key and the token only, never the payload.
 */
@Service
public class DeliveryService {

    private static final Logger log = LoggerFactory.getLogger(DeliveryService.class);

    private final DeliveryLedger ledger;
    private final FaultInjector faults;

    public DeliveryService(DeliveryLedger ledger, FaultInjector faults) {
        this.ledger = ledger;
        this.faults = faults;
    }

    /**
     * @throws InjectedFaultException when fault injection fails the request; nothing was written
     */
    public DeliveryResult deliver(DeliveryCommand cmd) {
        try {
            faults.apply();
        } catch (InjectedFaultException e) {
            log.info("delivery outcome=INJECTED_FAULT runId={} idemKeyHash={} fencingToken={} status={}",
                    cmd.runId(), Hashing.logHash(cmd.idemKey()), cmd.token(), e.status());
            throw e;
        }
        DeliveryResult result = ledger.deliver(cmd);
        log.info("delivery outcome={} runId={} idemKeyHash={} fencingToken={}",
                result.outcome(), cmd.runId(), Hashing.logHash(cmd.idemKey()), cmd.token());
        return result;
    }
}
