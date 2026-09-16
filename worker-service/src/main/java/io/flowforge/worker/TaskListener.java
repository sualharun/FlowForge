package io.flowforge.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.flowforge.shared.TaskJob;
import io.flowforge.shared.Topics;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class TaskListener {
    private final ObjectMapper json;
    private final WorkerRuntime runtime;

    public TaskListener(ObjectMapper json, WorkerRuntime runtime) {
        this.json = json;
        this.runtime = runtime;
    }

    @KafkaListener(topics = {Topics.READY, Topics.RETRY}, groupId = "flowforge-workers",
            concurrency = "${flowforge.worker.concurrency:4}")
    public void onTask(String message, Acknowledgment acknowledgment)
            throws JsonProcessingException, InterruptedException {
        TaskJob job = json.readValue(message, TaskJob.class);
        if (job == null || job.executionId() == null) {
            throw new IllegalArgumentException("Task dispatch must include an executionId");
        }
        // Only the execution ID is trusted; the claim returns authoritative persisted input.
        runtime.execute(job.executionId(), acknowledgment);
    }
}
