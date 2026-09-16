package io.flowforge.shared;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import java.util.Map;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class EngineMetrics {
    private final EngineStore store;
    private final MeterRegistry registry;
    private volatile Map<String,Object> snapshot=Map.of();
    public EngineMetrics(EngineStore store,MeterRegistry registry) { this.store=store;this.registry=registry; }
    @PostConstruct public void register() {
        Map.ofEntries(Map.entry("workflowsSubmitted","workflows_submitted"),Map.entry("workflowsCompleted","workflows_completed"),
                Map.entry("workflowsFailed","workflows_failed"),Map.entry("tasksPerMinute","tasks_per_minute"),
                Map.entry("taskSuccessRate","task_success_ratio"),Map.entry("retryRate","retry_ratio"),Map.entry("deadLetterCount","dead_letters"),
                Map.entry("queueDepth","queue_depth"),Map.entry("activeWorkers","active_workers"),Map.entry("p50TaskDurationMs","task_duration_p50_ms"),
                Map.entry("p95TaskDurationMs","task_duration_p95_ms"),Map.entry("pendingOutboxMessages","pending_outbox_messages"))
                .forEach((key,name)->Gauge.builder("flowforge_"+name,this,m->((Number)m.snapshot.getOrDefault(key,0)).doubleValue())
                        .description("Durable engine snapshot: "+key).register(registry));
    }
    @Scheduled(fixedDelayString="${flowforge.metrics.interval-ms:10000}")
    public void refresh() { snapshot=store.summary(); }
}
