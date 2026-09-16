package io.flowforge.worker;

import io.flowforge.shared.EngineStore;
import io.flowforge.shared.TaskJob;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class MockPaymentHandler implements TaskHandler {
    private final EngineStore store;

    public MockPaymentHandler(EngineStore store) {
        this.store = store;
    }

    @Override
    public String type() { return "MOCK_PAYMENT"; }

    @Override
    public Map<String, Object> execute(TaskJob job) throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Payment execution interrupted");
        }
        long failUntilAttempt = PayloadValues.nonNegativeInteger(job.payload(), "failUntilAttempt", 0);
        if (job.attemptNumber() <= failUntilAttempt) {
            throw new TaskExecutionException("Simulated transient payment provider failure", true);
        }
        // The store fences against cancelled/stale attempts and writes one ledger row per task.
        return store.mockPayment(job);
    }
}
