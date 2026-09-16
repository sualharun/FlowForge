package io.flowforge.shared;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record TaskJob(UUID workflowId, UUID taskId, UUID executionId, int attemptNumber,
        String taskType, Map<String, Object> payload, long timeoutMs, int maxRetries,
        long initialRetryDelayMs, double backoffMultiplier, Instant createdAt) {}
