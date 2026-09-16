package io.flowforge.shared;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.util.List;
import java.util.Map;

public record WorkflowDefinition(@NotBlank @Size(max=200) String name,
        @Min(1) @Max(1000) Integer concurrencyLimit,
        @NotEmpty @Size(max=1000) List<@Valid TaskDefinition> tasks) {
    public record TaskDefinition(@NotBlank @Size(max=200) String name,
            @NotBlank String taskType, Map<String,Object> payload,
            @Size(max=1000) List<@Size(max=200) String> dependsOn,
            @Min(10) @Max(3600000) Long timeoutMs, @Min(0) @Max(20) Integer maxRetries,
            @Min(10) @Max(3600000) Long initialRetryDelayMs, @DecimalMin("1.0") @DecimalMax("10.0") Double backoffMultiplier) {
        public long effectiveTimeout() { return timeoutMs == null ? 30000 : timeoutMs; }
        public int effectiveRetries() { return maxRetries == null ? 3 : maxRetries; }
        public long effectiveDelay() { return initialRetryDelayMs == null ? 1000 : initialRetryDelayMs; }
        public double effectiveBackoff() { return backoffMultiplier == null ? 2 : backoffMultiplier; }
        public List<String> parents() { return dependsOn == null ? List.of() : dependsOn; }
    }
}
