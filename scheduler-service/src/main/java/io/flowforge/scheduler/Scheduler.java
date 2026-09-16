package io.flowforge.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.flowforge.shared.*;
import java.util.UUID;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class Scheduler {
    private final EngineStore store;
    private final RedisCoordination coordination;
    private final ObjectMapper json;
    public Scheduler(EngineStore store,RedisCoordination coordination,ObjectMapper json) { this.store=store;this.coordination=coordination;this.json=json; }
    @Scheduled(fixedDelayString="${flowforge.scheduler.interval-ms:500}")
    public void tick() {
        String token=UUID.randomUUID().toString();
        if (!coordination.acquire(token)) return;
        try { store.markUnhealthyWorkers();store.scheduleOnce(); }
        finally { coordination.release(token); }
    }
    @KafkaListener(topics=Topics.RESULTS,groupId="flowforge-scheduler-results",concurrency="${flowforge.scheduler.result-concurrency:4}")
    public void result(String message) throws JsonProcessingException { store.applyResult(json.readValue(message,TaskResult.class).executionId()); }
}
