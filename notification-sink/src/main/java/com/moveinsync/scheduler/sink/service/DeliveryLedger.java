package com.moveinsync.scheduler.sink.service;

import com.moveinsync.scheduler.core.domain.DeliveryOutcome;
import com.moveinsync.scheduler.core.domain.FencingToken;
import com.moveinsync.scheduler.sink.domain.Delivery;
import com.moveinsync.scheduler.sink.domain.DeliveryCommand;
import com.moveinsync.scheduler.sink.domain.DeliveryResult;
import com.moveinsync.scheduler.sink.domain.FenceDecision;
import com.moveinsync.scheduler.sink.repository.AttemptLogRepository;
import com.moveinsync.scheduler.sink.repository.DeliveryRepository;
import com.moveinsync.scheduler.sink.repository.RunFenceRepository;
import java.util.Optional;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The single transaction behind every delivery (plan section 07, "Sink fencing"):
 *
 * <ol>
 *   <li>create and lock the run's fence row, serialising all requests for the run;
 *   <li>reject a token lower than the fence (nothing delivered);
 *   <li>otherwise raise the fence if the token is higher, then insert the delivery unless the key
 *       is already delivered;
 *   <li>log the attempt with its outcome, in every case.
 * </ol>
 *
 * <p>The stale verdict is a normal return, not an exception, so its attempt-log row commits.
 */
@Service
public class DeliveryLedger {

    private final RunFenceRepository fences;
    private final DeliveryRepository deliveries;
    private final AttemptLogRepository attempts;

    public DeliveryLedger(RunFenceRepository fences, DeliveryRepository deliveries, AttemptLogRepository attempts) {
        this.fences = fences;
        this.deliveries = deliveries;
        this.attempts = attempts;
    }

    @Transactional
    public DeliveryResult deliver(DeliveryCommand cmd) {
        FencingToken fence = fences.lockOrCreate(cmd.runId(), cmd.token());
        FenceDecision decision = FenceDecision.decide(fence, cmd.token());
        if (!decision.accepted()) {
            attempts.record(cmd, DeliveryOutcome.STALE_TOKEN);
            return DeliveryResult.stale();
        }
        if (decision == FenceDecision.ACCEPT_RAISE) {
            fences.raise(cmd.runId(), cmd.token());
        }
        Optional<Delivery> inserted = deliveries.insertIfAbsent(cmd);
        DeliveryResult result = inserted
                .map(d -> new DeliveryResult(DeliveryOutcome.DELIVERED, d))
                .orElseGet(() -> new DeliveryResult(DeliveryOutcome.ALREADY_DELIVERED, existing(cmd.idemKey())));
        attempts.record(cmd, result.outcome());
        return result;
    }

    private Delivery existing(String idemKey) {
        // The conflicting row is committed (ON CONFLICT waits for in-flight inserts), so it is visible.
        return deliveries.find(idemKey)
                .orElseThrow(() -> new IllegalStateException("delivery conflict without a visible row"));
    }
}
