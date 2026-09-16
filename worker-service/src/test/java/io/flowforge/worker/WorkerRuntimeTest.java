package io.flowforge.worker;

import static io.flowforge.worker.TaskHandlersTest.job;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.flowforge.shared.EngineStore;
import io.flowforge.shared.RedisCoordination;
import io.flowforge.shared.TaskJob;
import io.flowforge.shared.TaskResult;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.kafka.support.Acknowledgment;

@Timeout(10)
class WorkerRuntimeTest {
    private EngineStore store;
    private RedisCoordination redis;
    private TaskHandlerRegistry handlers;
    private WorkerRuntime runtime;
    private SimpleMeterRegistry metrics;
    private Acknowledgment acknowledgment;
    private final ExecutorService callers = Executors.newVirtualThreadPerTaskExecutor();

    @BeforeEach
    void setup() {
        store = mock(EngineStore.class);
        redis = mock(RedisCoordination.class);
        handlers = mock(TaskHandlerRegistry.class);
        acknowledgment = mock(Acknowledgment.class);
        metrics = new SimpleMeterRegistry();
        runtime = new WorkerRuntime(store, redis, handlers, metrics, 2, 10);
        when(store.isExecutionActive(any(), any())).thenReturn(true);
    }

    @AfterEach
    void cleanup() {
        runtime.close();
        callers.shutdownNow();
        metrics.close();
    }

    private void claim(TaskJob job) {
        when(store.claim(job.executionId(), runtime.workerId())).thenReturn(Optional.of(job));
    }

    private TaskResult recordedResult() {
        var result = ArgumentCaptor.forClass(TaskResult.class);
        verify(store).recordResult(result.capture());
        return result.getValue();
    }

    @Test
    void acknowledgesOnlyAfterClaimAndPersistsCanonicalJobResult() throws Exception {
        TaskJob job = job("DATA_TRANSFORM", Map.of("data", "order"), 1000, 1);
        claim(job);
        when(handlers.execute(job)).thenReturn(Map.of("data", "ORDER"));
        runtime.execute(job.executionId(), acknowledgment);
        var order = inOrder(store, acknowledgment, handlers);
        order.verify(store).claim(job.executionId(), runtime.workerId());
        order.verify(acknowledgment).acknowledge();
        order.verify(store).isExecutionActive(job.executionId(), runtime.workerId());
        order.verify(handlers).execute(job);
        order.verify(store).recordResult(any());
        TaskResult result = recordedResult();
        assertThat(result.status()).isEqualTo("COMPLETED");
        assertThat(result.output()).containsEntry("data", "ORDER");
        assertThat(result.workerId()).isEqualTo(runtime.workerId());
    }

    @Test
    void duplicateDeliveryNeverRunsAHandler() throws Exception {
        UUID executionId = UUID.randomUUID();
        when(store.claim(executionId, runtime.workerId())).thenReturn(Optional.empty());
        runtime.execute(executionId, acknowledgment);
        verify(acknowledgment).acknowledge();
        verifyNoInteractions(handlers);
        verify(store, never()).recordResult(any());
        assertThat(metrics.get("flowforge.worker.duplicate.deliveries").counter().count()).isEqualTo(1);
    }

    @Test
    void databaseClaimFailureDoesNotAcknowledgeKafkaRecord() {
        when(store.claim(any(), any())).thenThrow(new TransientDataAccessResourceException("unavailable"));
        assertThatThrownBy(() -> runtime.execute(UUID.randomUUID(), acknowledgment))
                .isInstanceOf(TransientDataAccessResourceException.class);
        verifyNoInteractions(acknowledgment, handlers);
    }

    @Test
    void timedOutHandlerIsInterruptedAndReportedAsRetryable() throws Exception {
        TaskJob job = job("DELAY", Map.of(), 100, 1);
        claim(job);
        CountDownLatch interrupted = new CountDownLatch(1);
        when(handlers.execute(job)).thenAnswer(invocation -> {
            try { Thread.sleep(10_000); }
            catch (InterruptedException expected) { interrupted.countDown(); throw expected; }
            return Map.of();
        });
        runtime.execute(job.executionId(), acknowledgment);
        assertThat(interrupted.await(1, TimeUnit.SECONDS)).isTrue();
        TaskResult result = recordedResult();
        assertThat(result.status()).isEqualTo("TIMED_OUT");
        assertThat(result.retryable()).isTrue();
        assertThat(result.error()).contains("100 ms");
    }

    @Test
    void cancellationInterruptsRunningHandler() throws Exception {
        TaskJob job = job("DELAY", Map.of(), 10_000, 1);
        claim(job);
        when(store.isExecutionActive(any(), any())).thenReturn(true, false);
        CountDownLatch interrupted = new CountDownLatch(1);
        when(handlers.execute(job)).thenAnswer(invocation -> {
            try { Thread.sleep(10_000); }
            catch (InterruptedException expected) { interrupted.countDown(); throw expected; }
            return Map.of();
        });
        runtime.execute(job.executionId(), acknowledgment);
        assertThat(interrupted.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(recordedResult().status()).isEqualTo("CANCELLED");
    }

    @Test
    void cancelledBeforeExecutionNeverStartsHandler() throws Exception {
        TaskJob job = job("DELAY", Map.of(), 1000, 1);
        claim(job);
        when(store.isExecutionActive(any(), any())).thenReturn(false);
        runtime.execute(job.executionId(), acknowledgment);
        verifyNoInteractions(handlers);
        assertThat(recordedResult().status()).isEqualTo("CANCELLED");
    }

    @Test
    void handlerFailurePreservesRetryClassification() throws Exception {
        TaskJob job = job("MOCK_PAYMENT", Map.of(), 1000, 1);
        claim(job);
        when(handlers.execute(job)).thenThrow(new TaskExecutionException("temporary", true));
        runtime.execute(job.executionId(), acknowledgment);
        assertThat(recordedResult()).satisfies(result -> {
            assertThat(result.status()).isEqualTo("FAILED");
            assertThat(result.retryable()).isTrue();
        });
    }

    @Test
    void invalidPaymentPayloadIsNotRetried() throws Exception {
        TaskJob job = job("MOCK_PAYMENT", Map.of("amount", -1), 1000, 1);
        claim(job);
        when(handlers.execute(job)).thenThrow(new IllegalArgumentException("Payment amount must be positive"));
        runtime.execute(job.executionId(), acknowledgment);
        assertThat(recordedResult().retryable()).isFalse();
    }

    @Test
    void boundedConcurrencyKeepsHeartbeatIndependentOfHandlers() throws Exception {
        List<TaskJob> jobs = java.util.stream.IntStream.range(0, 4)
                .mapToObj(i -> job("DELAY", Map.of(), 5000, 1)).toList();
        jobs.forEach(this::claim);
        CountDownLatch firstTwoStarted = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger running = new AtomicInteger();
        AtomicInteger maximum = new AtomicInteger();
        when(handlers.execute(any())).thenAnswer(invocation -> {
            maximum.accumulateAndGet(running.incrementAndGet(), Math::max);
            firstTwoStarted.countDown();
            try { release.await(); return Map.of(); }
            finally { running.decrementAndGet(); }
        });
        List<Future<?>> executions = new ArrayList<>();
        for (TaskJob job : jobs) {
            executions.add(callers.submit(() -> { runtime.execute(job.executionId(), mock(Acknowledgment.class)); return null; }));
        }
        assertThat(firstTwoStarted.await(2, TimeUnit.SECONDS)).isTrue();
        runtime.heartbeat();
        verify(store).heartbeat(runtime.workerId(), 2);
        verify(redis).cacheWorker(runtime.workerId(), 2);
        verify(store, times(2)).claim(any(), any());
        release.countDown();
        for (Future<?> execution : executions) { execution.get(2, TimeUnit.SECONDS); }
        assertThat(maximum.get()).isEqualTo(2);
        verify(store, times(4)).recordResult(any());
        runtime.heartbeat();
        verify(store).heartbeat(runtime.workerId(), 0);
    }

    @Test
    void transientResultWriteIsRetriedWithTheSameResult() throws Exception {
        TaskJob job = job("DELAY", Map.of(), 1000, 1);
        claim(job);
        when(handlers.execute(job)).thenReturn(Map.of());
        doThrow(new TransientDataAccessResourceException("temporary write failure")).doNothing().when(store).recordResult(any());
        runtime.execute(job.executionId(), acknowledgment);
        var results = ArgumentCaptor.forClass(TaskResult.class);
        verify(store, times(2)).recordResult(results.capture());
        assertThat(results.getAllValues().get(0)).isSameAs(results.getAllValues().get(1));
    }

    @Test
    void repeatedResultWriteFailureRemainsVisibleForRecovery() throws Exception {
        TaskJob job = job("DELAY", Map.of(), 1000, 1);
        claim(job);
        when(handlers.execute(job)).thenReturn(Map.of());
        doThrow(new TransientDataAccessResourceException("database unavailable")).when(store).recordResult(any());
        assertThatThrownBy(() -> runtime.execute(job.executionId(), acknowledgment))
                .isInstanceOf(TransientDataAccessResourceException.class);
        verify(store, times(3)).recordResult(any());
    }

    @Test
    void offsetCommitFailureDoesNotLoseDurablyClaimedWork() throws Exception {
        TaskJob job = job("DELAY", Map.of(), 1000, 1);
        claim(job);
        when(handlers.execute(job)).thenReturn(Map.of());
        doThrow(new IllegalStateException("partition revoked")).when(acknowledgment).acknowledge();
        assertThatThrownBy(() -> runtime.execute(job.executionId(), acknowledgment))
                .isInstanceOf(IllegalStateException.class).hasMessage("partition revoked");
        verify(handlers).execute(job);
        assertThat(recordedResult().status()).isEqualTo("COMPLETED");
    }
}
