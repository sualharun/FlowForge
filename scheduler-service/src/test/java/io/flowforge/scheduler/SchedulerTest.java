package io.flowforge.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.json.JsonMapper;
import io.flowforge.shared.EngineStore;
import io.flowforge.shared.RedisCoordination;
import io.flowforge.shared.TaskResult;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class SchedulerTest {
    private final JsonMapper json = JsonMapper.builder().findAndAddModules().build();

    @Test
    void tickWithoutTheLeaseDoesNoDatabaseWork() {
        var store = mock(EngineStore.class);
        var coordination = mock(RedisCoordination.class);
        when(coordination.acquire(anyString())).thenReturn(false);

        new Scheduler(store, coordination, json).tick();

        // The lease only suppresses redundant scans, so a lost race must cost nothing.
        verifyNoInteractions(store);
        verify(coordination, never()).release(anyString());
    }

    @Test
    void tickRecoversLostWorkBeforeAdmittingNewWork() {
        var store = mock(EngineStore.class);
        var coordination = mock(RedisCoordination.class);
        when(coordination.acquire(anyString())).thenReturn(true);

        new Scheduler(store, coordination, json).tick();

        // Abandoned executions must be failed/requeued before fresh tasks take admission slots.
        var order = inOrder(store);
        order.verify(store).markUnhealthyWorkers();
        order.verify(store).scheduleOnce();
        order.verifyNoMoreInteractions();
    }

    @Test
    void tickReleasesTheSameTokenItAcquired() {
        var store = mock(EngineStore.class);
        var coordination = mock(RedisCoordination.class);
        when(coordination.acquire(anyString())).thenReturn(true);

        new Scheduler(store, coordination, json).tick();

        var acquired = ArgumentCaptor.forClass(String.class);
        var released = ArgumentCaptor.forClass(String.class);
        verify(coordination).acquire(acquired.capture());
        verify(coordination).release(released.capture());
        // Releasing a different token would either leak this lease or delete a peer's.
        assertThat(released.getValue()).isEqualTo(acquired.getValue());
    }

    @Test
    void failedTickStillReleasesTheLease() {
        var store = mock(EngineStore.class);
        var coordination = mock(RedisCoordination.class);
        when(coordination.acquire(anyString())).thenReturn(true);
        doThrow(new IllegalStateException("database unavailable")).when(store).scheduleOnce();

        assertThatThrownBy(() -> new Scheduler(store, coordination, json).tick())
                .isInstanceOf(IllegalStateException.class);

        // Otherwise a repeatedly failing replica would hold the lease until it expired.
        verify(coordination).release(anyString());
    }

    @Test
    void resultListenerTrustsOnlyTheExecutionIdFromTheBroker() throws Exception {
        var store = mock(EngineStore.class);
        var coordination = mock(RedisCoordination.class);
        UUID executionId = UUID.randomUUID();
        // Status, output, and error are deliberately misleading: the canonical result is the
        // row the worker already committed, which applyResult re-reads under the row locks.
        var tampered = new TaskResult(UUID.randomUUID(), UUID.randomUUID(), executionId, UUID.randomUUID(),
                "COMPLETED", false, Map.of("amount", "tampered"), null, Instant.now());

        new Scheduler(store, coordination, json).result(json.writeValueAsString(tampered));

        verify(store).applyResult(executionId);
        verifyNoMoreInteractions(store);
        verifyNoInteractions(coordination);
    }

    @Test
    void malformedResultNotificationIsRejectedWithoutTouchingState() {
        var store = mock(EngineStore.class);
        var coordination = mock(RedisCoordination.class);
        var scheduler = new Scheduler(store, coordination, json);

        assertThatThrownBy(() -> scheduler.result("not json")).isInstanceOf(Exception.class);

        verifyNoInteractions(store, coordination);
    }
}
