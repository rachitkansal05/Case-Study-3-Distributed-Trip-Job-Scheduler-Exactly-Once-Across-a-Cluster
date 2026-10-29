package com.moveinsync.scheduler.core.ports;

import com.moveinsync.scheduler.core.domain.ClaimedRun;
import com.moveinsync.scheduler.core.domain.FencingToken;
import com.moveinsync.scheduler.core.domain.RunFailure;
import com.moveinsync.scheduler.core.domain.TenantId;
import java.util.List;
import java.util.Set;

/**
 * The only writer of run state and tenant slots (plan section 07). Polling, the orphan sweep,
 * manual triggers and dead-letter replays all go through the implementation, so no path can skip
 * these rules. Implemented by the Engine team ({@code JdbcClaimProtocol}).
 *
 * <h2>Rules every implementation must keep</h2>
 *
 * <ol>
 *   <li><b>DB time only.</b> Every due/lease comparison uses {@code clock_timestamp()}, never
 *       {@code now()} (transaction start) and never the node clock.
 *   <li><b>Lock order.</b> Slot row first, then run row. Commits, renewals and checkpoints never
 *       lock slot rows, so claims cannot deadlock with each other or with commits.
 *   <li><b>Fenced predicate.</b> Every write by a holder ({@link #renew}, {@link #checkpoint},
 *       {@link #commitSuccess}, {@link #commitFailure}) is conditional on
 *       <pre>{@code
 * WHERE id = :run AND (fencing_epoch, fencing_seq) = (:e, :s)
 *   AND state = 'RUNNING' AND lease_until >= clock_timestamp()
 * }</pre>
 *       and reports 0 rows as {@code false} (lease lost). <b>An expired lease can never be
 *       renewed, checkpointed or committed</b>, even if nobody has taken the run over yet.
 *   <li><b>Slot release is part of the fenced statement</b> ({@code slot_no = NULL} in the same
 *       UPDATE), so a late commit from a zombie can never free a slot that now belongs to another
 *       run.
 *   <li><b>Tokens.</b> Every claim sets {@code fencing_seq = nextval('fence_seq')} and {@code
 *       fencing_epoch = cluster_generation * 2^32 + timeline} (see {@link
 *       FencingToken#epochOf(int, long)}), read on the claiming connection. If the timeline read
 *       is NULL or not positive the claim fails closed and starts nothing. Returned tokens are
 *       usable only because the claim transaction has committed before the method returns.
 *   <li><b>Attempts.</b> A claim increments {@code attempt}. Reaping an expired holder is a lease
 *       loss, not a failure: it does not count toward {@code max_attempts} but increments {@code
 *       lease_lost_count}; past the lease-lost limit the run is dead-lettered with {@code
 *       FENCING_CONFLICT}.
 * </ol>
 *
 * <p>A {@code false} result from any fenced method means the caller has lost the run: it must stop
 * the handler, send nothing more, count {@code scheduler_lease_lost_total}, and (for commits)
 * raise {@code StaleFencingTokenException} / count {@code scheduler_stale_commit_rejected_total}.
 *
 * <p>Infrastructure failures (database unreachable) are thrown as unchecked exceptions.
 */
public interface ClaimProtocol {

    /**
     * Claims up to {@code maxRuns} due runs of one tenant in the given partitions and starts each in
     * a free slot of that tenant, in one transaction (plan section 07, steps 1-3):
     *
     * <ol>
     *   <li>Lock up to {@code maxRuns} slot rows of {@code tenant} that look free ({@code FOR UPDATE
     *       OF s SKIP LOCKED}); the "looks free" filter is only a hint.
     *   <li>For each locked slot that still has an expired holder, reap it with a conditional
     *       {@code UPDATE ... WHERE state = 'RUNNING' AND lease_until < clock_timestamp()}: 1 row
     *       means the slot is free, 0 rows means the holder renewed in time and the slot is skipped.
     *       Exactly one of a concurrent renew and reap succeeds.
     *   <li>Start one due run ({@code SCHEDULED}/{@code RETRY_WAIT}, {@code due_at <=
     *       clock_timestamp()}) per free slot, ordered by {@code priority, due_at}, picked with
     *       {@code FOR UPDATE SKIP LOCKED}; set {@code RUNNING}, slot, owner, token, {@code
     *       lease_until = clock_timestamp() + runLease}.
     * </ol>
     *
     * Skips the tenant entirely if {@code tenant.paused} (read directly, never cached). Applies the
     * job's misfire policy to runs later than their threshold; runs it skips are not returned.
     * Nothing is claimed that cannot start right away.
     *
     * @param nodeId this node, written to {@code owner_node}
     * @param tenant tenant to claim for (claims are per tenant, so one tenant cannot fill a batch)
     * @param ownedPartitions partitions this node currently polls; correctness does not depend on it
     * @param maxRuns upper bound on runs started (usually free local workers for this tenant)
     * @return the started runs, already committed; empty if none
     */
    List<ClaimedRun> claimDue(String nodeId, TenantId tenant, Set<Integer> ownedPartitions, int maxRuns);

    /**
     * Orphan sweep, in <b>any</b> partition: finds {@code RUNNING} runs whose lease expired (index
     * {@code run_expired}), reaps each with the conditional update above and restarts it in the
     * <b>same slot</b> with a higher token, keeping its checkpoint, in one claim transaction per
     * run. Returned runs have {@link ClaimedRun#recovered()} {@code = true}.
     *
     * @param nodeId this node
     * @param maxRuns upper bound on runs taken over
     * @return the taken-over runs, already committed
     */
    List<ClaimedRun> sweepOrphans(String nodeId, int maxRuns);

    /**
     * Extends the lease to {@code clock_timestamp() + runLease} under the fenced predicate.
     *
     * @return {@code true} if renewed; {@code false} = lease lost (expired or taken over)
     */
    boolean renew(long runId, FencingToken token);

    /**
     * Stores a checkpoint (e.g. {@code {"nextItem": 650}}) under the fenced predicate, so a stale
     * node can never move a checkpoint backward.
     *
     * @param checkpointJson JSON document; never contains personal data
     * @return {@code true} if stored; {@code false} = lease lost
     */
    boolean checkpoint(long runId, FencingToken token, String checkpointJson);

    /**
     * Marks the run {@code SUCCEEDED}, sets {@code finished_at} and frees the slot ({@code slot_no
     * = NULL}) in one fenced statement.
     *
     * @return {@code true} if committed; {@code false} = lease lost, the commit was rejected and
     *     the run will execute again (its effects are deduplicated by the sink)
     */
    boolean commitSuccess(long runId, FencingToken token);

    /**
     * Applies a failed attempt under the fenced predicate and frees the slot in the same statement:
     * {@link RunFailure.Retry} sets {@code RETRY_WAIT} with {@code due_at = clock_timestamp() +
     * delay}; {@link RunFailure.DeadLetter} sets {@code DEAD_LETTERED} and inserts the {@code
     * dead_letter} row in the same transaction. {@code last_error} gets the error code and safe
     * message only.
     *
     * @return {@code true} if applied; {@code false} = lease lost (nothing written)
     */
    boolean commitFailure(long runId, FencingToken token, RunFailure failure);
}
