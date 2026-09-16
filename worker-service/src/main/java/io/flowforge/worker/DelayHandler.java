package io.flowforge.worker;

import io.flowforge.shared.TaskJob;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class DelayHandler implements TaskHandler {
    @Override
    public String type() { return "DELAY"; }

    @Override
    public Map<String, Object> execute(TaskJob job) throws InterruptedException {
        String key = job.payload().containsKey("durationMs") ? "durationMs" : "delayMs";
        long durationMs = PayloadValues.nonNegativeInteger(job.payload(), key, 0);
        Thread.sleep(durationMs);
        return Map.of("sleptMs", durationMs);
    }
}
