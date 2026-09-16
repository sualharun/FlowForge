package io.flowforge.worker;

import io.flowforge.shared.TaskJob;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class DataTransformHandler implements TaskHandler {
    @Override
    public String type() { return "DATA_TRANSFORM"; }

    @Override
    public Map<String, Object> execute(TaskJob job) {
        Map<String, Object> payload = job.payload();
        if (!payload.containsKey("data")) {
            throw new TaskExecutionException("DATA_TRANSFORM requires data", false);
        }
        Object operationValue = payload.getOrDefault("operation", "IDENTITY");
        if (!(operationValue instanceof String operation)) {
            throw new TaskExecutionException("operation must be a string", false);
        }
        Object data = payload.get("data");
        Object transformed = switch (operation) {
            case "IDENTITY" -> data;
            case "UPPERCASE", "LOWERCASE" -> {
                if (!(data instanceof String text)) {
                    throw new TaskExecutionException(operation + " requires string data", false);
                }
                yield "UPPERCASE".equals(operation) ? text.toUpperCase(Locale.ROOT) : text.toLowerCase(Locale.ROOT);
            }
            default -> throw new TaskExecutionException("Unsupported transform operation: " + operation, false);
        };
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("data", transformed);
        return output;
    }
}
