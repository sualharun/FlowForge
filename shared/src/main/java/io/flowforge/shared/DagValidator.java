package io.flowforge.shared;

import java.util.*;
import static io.flowforge.shared.WorkflowDefinition.TaskDefinition;

/** Kahn's algorithm validates the entire graph in O(vertices + edges), without recursive stack growth. */
public final class DagValidator {
    private static final Set<String> TYPES = Set.of("DELAY", "DATA_TRANSFORM", "MOCK_PAYMENT");
    public static final int MAX_EDGES = 20000;
    private DagValidator() {}
    public static void validate(WorkflowDefinition definition) {
        if (definition.tasks() == null || definition.tasks().isEmpty() || definition.tasks().size() > 1000)
            throw new IllegalArgumentException("A workflow requires between 1 and 1000 tasks");
        Map<String,TaskDefinition> tasks = new LinkedHashMap<>();
        for (TaskDefinition task : definition.tasks()) {
            if (task == null || task.name() == null || task.name().isBlank()) throw new IllegalArgumentException("Task name is required");
            if (tasks.putIfAbsent(task.name(),task) != null) throw new IllegalArgumentException("Duplicate task name: " + task.name());
            if (!TYPES.contains(task.taskType())) throw new IllegalArgumentException("Unsupported task type: " + task.taskType());
            if (task.effectiveTimeout() < 10 || task.effectiveTimeout() > 3600000 || task.effectiveRetries() < 0 || task.effectiveRetries() > 20
                    || task.effectiveDelay() < 10 || task.effectiveDelay() > 3600000 || !Double.isFinite(task.effectiveBackoff())
                    || task.effectiveBackoff() < 1 || task.effectiveBackoff() > 10) throw new IllegalArgumentException("Invalid retry or timeout configuration");
        }
        Map<String,Integer> indegree = new HashMap<>();
        Map<String,List<String>> children = new HashMap<>();
        // 1000 tasks alone permit ~499,500 edges, and every edge becomes a persisted row.
        // Bounding the total keeps one accepted submission from dominating a transaction.
        int edges = 0;
        for (TaskDefinition task : tasks.values()) {
            Set<String> unique = new HashSet<>(task.parents());
            if (unique.size() != task.parents().size()) throw new IllegalArgumentException("Duplicate dependencies: " + task.name());
            if ((edges += unique.size()) > MAX_EDGES)
                throw new IllegalArgumentException("A workflow may declare at most " + MAX_EDGES + " dependency edges");
            indegree.put(task.name(),unique.size());
            for (String parent : unique) {
                if (!tasks.containsKey(parent)) throw new IllegalArgumentException("Unknown dependency: " + parent);
                children.computeIfAbsent(parent,k->new ArrayList<>()).add(task.name());
            }
        }
        Deque<String> ready = new ArrayDeque<>();
        indegree.forEach((task,degree)-> { if (degree == 0) ready.add(task); });
        int visited = 0;
        while (!ready.isEmpty()) {
            String task = ready.remove(); visited++;
            for (String child : children.getOrDefault(task,List.of())) if (indegree.compute(child,(key,value)->value-1) == 0) ready.add(child);
        }
        if (visited != tasks.size()) throw new IllegalArgumentException("Workflow contains a dependency cycle");
    }
}
