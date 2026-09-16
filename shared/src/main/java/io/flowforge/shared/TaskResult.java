package io.flowforge.shared;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record TaskResult(UUID workflowId, UUID taskId, UUID executionId, UUID workerId,
        String status, boolean retryable, Map<String, Object> output, String error, Instant completedAt) {}
