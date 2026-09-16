package io.flowforge.worker;

import io.flowforge.shared.TaskJob;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class TaskHandlerRegistry {
    private final Map<String, TaskHandler> handlers;

    public TaskHandlerRegistry(List<TaskHandler> candidates) {
        Map<String, TaskHandler> registry = new HashMap<>();
        for (TaskHandler handler : candidates) {
            if (registry.putIfAbsent(handler.type(), handler) != null) {
                throw new IllegalArgumentException("Duplicate task handler: " + handler.type());
            }
        }
        handlers = Map.copyOf(registry);
    }

    public Map<String, Object> execute(TaskJob job) throws InterruptedException {
        TaskHandler handler = handlers.get(job.taskType());
        if (handler == null) {
            throw new TaskExecutionException("Unsupported task type: " + job.taskType(), false);
        }
        return handler.execute(job);
    }
}
