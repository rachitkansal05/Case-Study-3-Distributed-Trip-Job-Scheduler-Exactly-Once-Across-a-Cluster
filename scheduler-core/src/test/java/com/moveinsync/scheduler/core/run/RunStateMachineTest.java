package com.moveinsync.scheduler.core.run;

import static com.moveinsync.scheduler.core.domain.RunState.CANCELLED;
import static com.moveinsync.scheduler.core.domain.RunState.DEAD_LETTERED;
import static com.moveinsync.scheduler.core.domain.RunState.PAUSED;
import static com.moveinsync.scheduler.core.domain.RunState.RETRY_WAIT;
import static com.moveinsync.scheduler.core.domain.RunState.RUNNING;
import static com.moveinsync.scheduler.core.domain.RunState.SCHEDULED;
import static com.moveinsync.scheduler.core.domain.RunState.SKIPPED;
import static com.moveinsync.scheduler.core.domain.RunState.SUCCEEDED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.moveinsync.scheduler.core.domain.RunState;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/** Plan section 04: the run state machine, checked over all 8 x 8 state pairs. */
class RunStateMachineTest {

    /** Expected legal transitions, written out independently of the implementation. */
    private static final Set<String> EXPECTED = Set.of(
            "SCHEDULED->RUNNING",
            "SCHEDULED->SKIPPED",
            "SCHEDULED->PAUSED",
            "SCHEDULED->CANCELLED",
            "PAUSED->SCHEDULED",
            "PAUSED->CANCELLED",
            "RUNNING->SUCCEEDED",
            "RUNNING->RETRY_WAIT",
            "RUNNING->DEAD_LETTERED",
            "RETRY_WAIT->RUNNING",
            "RETRY_WAIT->CANCELLED",
            "DEAD_LETTERED->SCHEDULED");

    static Stream<Arguments> allPairs() {
        return Stream.of(RunState.values())
                .flatMap(from -> Stream.of(RunState.values()).map(to -> Arguments.of(from, to)));
    }

    @Test
    void matrixCoversAllSixtyFourPairs() {
        assertThat(allPairs().count()).isEqualTo(64);
        assertThat(RunState.values()).hasSize(8);
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("allPairs")
    void transitionMatchesExpectedTable(RunState from, RunState to) {
        boolean expected = EXPECTED.contains(from + "->" + to);
        assertThat(RunStateMachine.isLegal(from, to)).isEqualTo(expected);
        assertThat(RunStateMachine.allowedFrom(from).contains(to)).isEqualTo(expected);
        if (expected) {
            assertThatCode(() -> RunStateMachine.requireTransition(from, to)).doesNotThrowAnyException();
        } else {
            assertThatThrownBy(() -> RunStateMachine.requireTransition(from, to))
                    .isInstanceOfSatisfying(IllegalRunTransitionException.class, e -> {
                        assertThat(e.from()).isEqualTo(from);
                        assertThat(e.to()).isEqualTo(to);
                    })
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(from.name())
                    .hasMessageContaining(to.name());
        }
    }

    @ParameterizedTest
    @EnumSource(RunState.class)
    void noSelfTransitions(RunState state) {
        assertThat(RunStateMachine.isLegal(state, state)).isFalse();
    }

    @ParameterizedTest
    @EnumSource(RunState.class)
    void terminalStatesAreConsistentWithRunState(RunState state) {
        Set<RunState> exits = RunStateMachine.allowedFrom(state);
        if (state == DEAD_LETTERED) {
            assertThat(state.isTerminal()).isTrue();
            assertThat(exits).containsExactly(SCHEDULED);
        } else if (state.isTerminal()) {
            assertThat(exits).isEmpty();
        } else {
            assertThat(exits).isNotEmpty();
        }
    }

    @Test
    void terminalWithoutExitsAreExactlySucceededSkippedCancelled() {
        EnumSet<RunState> noExits = EnumSet.noneOf(RunState.class);
        for (RunState s : RunState.values()) {
            if (RunStateMachine.allowedFrom(s).isEmpty()) {
                noExits.add(s);
            }
        }
        assertThat(noExits).containsExactlyInAnyOrder(SUCCEEDED, SKIPPED, CANCELLED);
    }

    @ParameterizedTest
    @EnumSource(RunState.class)
    void everyClaimableStateGoesDirectlyToRunning(RunState state) {
        if (state.isClaimable()) {
            assertThat(RunStateMachine.isLegal(state, RUNNING)).isTrue();
        }
    }

    @ParameterizedTest
    @EnumSource(value = RunState.class, names = {"SCHEDULED", "PAUSED", "RETRY_WAIT", "DEAD_LETTERED"})
    void runningIsReachableFromEveryNonFinalState(RunState start) {
        assertThat(reachable(start)).contains(RUNNING);
    }

    @Test
    void onlyRunningReachesSucceeded() {
        for (RunState s : RunState.values()) {
            assertThat(RunStateMachine.isLegal(s, SUCCEEDED)).isEqualTo(s == RUNNING);
        }
    }

    @Test
    void runningCannotBeCancelledOrPaused() {
        assertThat(RunStateMachine.isLegal(RUNNING, CANCELLED)).isFalse();
        assertThat(RunStateMachine.isLegal(RUNNING, PAUSED)).isFalse();
        assertThat(RunStateMachine.isLegal(RETRY_WAIT, PAUSED)).isFalse();
    }

    @Test
    void allowedFromIsUnmodifiable() {
        Set<RunState> s = RunStateMachine.allowedFrom(SCHEDULED);
        assertThatThrownBy(() -> s.add(SUCCEEDED)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> RunStateMachine.allowedFrom(SUCCEEDED).add(SCHEDULED))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(RunStateMachine.isLegal(SCHEDULED, SUCCEEDED)).isFalse();
    }

    @Test
    void nullsRejected() {
        assertThatThrownBy(() -> RunStateMachine.isLegal(null, RUNNING)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> RunStateMachine.isLegal(SCHEDULED, null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> RunStateMachine.allowedFrom(null)).isInstanceOf(NullPointerException.class);
    }

    private static Set<RunState> reachable(RunState start) {
        EnumSet<RunState> seen = EnumSet.noneOf(RunState.class);
        Deque<RunState> todo = new ArrayDeque<>();
        todo.add(start);
        while (!todo.isEmpty()) {
            for (RunState next : RunStateMachine.allowedFrom(todo.poll())) {
                if (seen.add(next)) {
                    todo.add(next);
                }
            }
        }
        return seen;
    }
}
