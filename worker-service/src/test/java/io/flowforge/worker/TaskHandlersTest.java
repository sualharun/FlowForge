package io.flowforge.worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

import io.flowforge.shared.EngineStore;
import io.flowforge.shared.TaskJob;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TaskHandlersTest {
    static TaskJob job(String type, Map<String, Object> payload, long timeoutMs, int attempt) {
        return new TaskJob(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), attempt,
                type, payload, timeoutMs, 3, 1000, 2, Instant.now());
    }

    @Test
    void transformsTextAndPreservesStructuredIdentity() {
        var handler = new DataTransformHandler();
        assertThat(handler.execute(job("DATA_TRANSFORM", Map.of("operation", "UPPERCASE", "data", "Order-i"), 1000, 1)))
                .containsEntry("data", "ORDER-I");
        assertThat(handler.execute(job("DATA_TRANSFORM", Map.of("operation", "LOWERCASE", "data", "ORDER"), 1000, 1)))
                .containsEntry("data", "order");
        Map<String, Object> object = Map.of("customer", List.of("a", "b"));
        assertThat(handler.execute(job("DATA_TRANSFORM", Map.of("data", object), 1000, 1)))
                .containsEntry("data", object);
        Map<String, Object> nullable = new HashMap<>();
        nullable.put("data", null);
        assertThat(handler.execute(job("DATA_TRANSFORM", nullable, 1000, 1))).containsEntry("data", null);
    }

    @Test
    void rejectsInvalidTransformInputAsPermanentFailure() {
        var handler = new DataTransformHandler();
        assertThatThrownBy(() -> handler.execute(job("DATA_TRANSFORM", Map.of("operation", "UPPERCASE", "data", 2), 1000, 1)))
                .isInstanceOfSatisfying(TaskExecutionException.class, error -> assertThat(error.retryable()).isFalse());
        assertThatThrownBy(() -> handler.execute(job("DATA_TRANSFORM", Map.of("operation", "EXEC", "data", "x"), 1000, 1)))
                .hasMessageContaining("Unsupported transform");
    }

    @Test
    void delayRejectsNegativeAndFractionalDurationsWithoutTruncation() throws InterruptedException {
        var handler = new DelayHandler();
        assertThat(handler.execute(job("DELAY", Map.of("durationMs", 0), 1000, 1)))
                .containsEntry("sleptMs", 0L);
        for (Object invalid : List.of(-1, 0.5, "100")) {
            assertThatThrownBy(() -> handler.execute(job("DELAY", Map.of("durationMs", invalid), 1000, 1)))
                    .isInstanceOfSatisfying(TaskExecutionException.class, error -> assertThat(error.retryable()).isFalse());
        }
    }

    @Test
    void paymentFailureSimulationOnlyCallsLedgerAfterRecovery() throws InterruptedException {
        EngineStore store = mock(EngineStore.class);
        var handler = new MockPaymentHandler(store);
        Map<String, Object> payload = Map.of("amount", "29.95", "failUntilAttempt", 2);
        for (int attempt : List.of(1, 2)) {
            assertThatThrownBy(() -> handler.execute(job("MOCK_PAYMENT", payload, 1000, attempt)))
                    .isInstanceOfSatisfying(TaskExecutionException.class, error -> assertThat(error.retryable()).isTrue());
        }
        verifyNoInteractions(store);
        TaskJob successfulAttempt = job("MOCK_PAYMENT", payload, 1000, 3);
        when(store.mockPayment(successfulAttempt)).thenReturn(Map.of("paymentId", "stable-payment"));
        assertThat(handler.execute(successfulAttempt)).containsEntry("paymentId", "stable-payment");
        verify(store).mockPayment(successfulAttempt);
    }

    @Test
    void registryRejectsDuplicateAndUnsupportedHandlers() {
        assertThatThrownBy(() -> new TaskHandlerRegistry(List.of(new DelayHandler(), new DelayHandler())))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Duplicate");
        var registry = new TaskHandlerRegistry(List.of(new DelayHandler()));
        assertThatThrownBy(() -> registry.execute(job("SHELL", Map.of(), 1000, 1)))
                .isInstanceOfSatisfying(TaskExecutionException.class, error -> assertThat(error.retryable()).isFalse());
    }
}
