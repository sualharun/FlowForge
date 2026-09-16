package io.flowforge.worker;

import io.flowforge.shared.TaskJob;
import java.util.Map;

/** Handlers must cooperate with interruption and use fenced, idempotent side effects. */
public interface TaskHandler {
    String type();
    Map<String, Object> execute(TaskJob job) throws InterruptedException;
}
