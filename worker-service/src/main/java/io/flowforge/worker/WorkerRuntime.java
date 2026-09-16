package io.flowforge.worker;

import io.flowforge.shared.EngineStore;
import io.flowforge.shared.RedisCoordination;
import io.flowforge.shared.TaskJob;
import io.flowforge.shared.TaskResult;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * A listener owns its claimed task until a result is durable. Handler work runs on bounded virtual
 * threads so timeout/cancellation can interrupt it while heartbeats use an independent scheduler.
 * PostgreSQL fences every claim and result. The scheduler recovers an acknowledged task after a crash
 * or a failed result write using its persisted deadline and worker heartbeat.
 */
@Component
public class WorkerRuntime {
    private static final Logger log = LoggerFactory.getLogger(WorkerRuntime.class);
    private final UUID workerId = UUID.randomUUID();
    private final EngineStore store;
    private final RedisCoordination redis;
    private final TaskHandlerRegistry handlers;
    private final MeterRegistry metrics;
    private final Semaphore slots;
    private final ThreadPoolExecutor executor;
    private final long cancellationPollMs;
    private final AtomicInteger activeTasks = new AtomicInteger();
    private final AtomicBoolean accepting = new AtomicBoolean(true);

    public WorkerRuntime(EngineStore store, RedisCoordination redis, TaskHandlerRegistry handlers, MeterRegistry metrics,
            @Value("${flowforge.worker.concurrency:4}") int concurrency,
            @Value("${flowforge.worker.cancellation-poll-ms:250}") long cancellationPollMs) {
        if (concurrency < 1 || concurrency > 1024) {
            throw new IllegalArgumentException("Worker concurrency must be between 1 and 1024");
        }
        if (cancellationPollMs < 1 || cancellationPollMs > 500) {
            throw new IllegalArgumentException("Cancellation polling interval must be between 1 and 500 ms");
        }
        this.store = store;
        this.redis = redis;
        this.handlers = handlers;
        this.metrics = metrics;
        this.cancellationPollMs = cancellationPollMs;
        slots = new Semaphore(concurrency, true);
        executor = new ThreadPoolExecutor(concurrency, concurrency, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(concurrency),
                Thread.ofVirtual().name("flowforge-handler-", 0).factory(),
                new ThreadPoolExecutor.AbortPolicy());
        metrics.gauge("flowforge.worker.active.tasks", activeTasks);
        log.atInfo().addKeyValue("workerId", workerId).addKeyValue("concurrency", concurrency)
                .log("Worker process started");
    }

    public UUID workerId() { return workerId; }

    @Scheduled(fixedDelayString = "${flowforge.worker.heartbeat-ms:2000}")
    public void heartbeat() {
        try {
            store.heartbeat(workerId, activeTasks.get());
            redis.cacheWorker(workerId, activeTasks.get());
        } catch (RuntimeException failure) {
            metrics.counter("flowforge.worker.heartbeat.failures").increment();
            log.atError().addKeyValue("workerId", workerId)
                    .setCause(failure).log("Worker heartbeat could not be persisted");
            throw failure;
        }
    }

    public void execute(UUID executionId, Acknowledgment acknowledgment) throws InterruptedException {
        slots.acquire();
        boolean claimed = false;
        try {
            if (!accepting.get()) {
                throw new RejectedExecutionException("Worker is shutting down");
            }
            Optional<TaskJob> claim = store.claim(executionId, workerId);
            if (claim.isEmpty()) {
                acknowledgment.acknowledge();
                metrics.counter("flowforge.worker.duplicate.deliveries").increment();
                return;
            }
            TaskJob job = claim.orElseThrow();
            claimed = true;
            activeTasks.incrementAndGet();
            log.atInfo().addKeyValue("workflowId", job.workflowId()).addKeyValue("taskId", job.taskId())
                    .addKeyValue("executionId", executionId).addKeyValue("workerId", workerId)
                    .addKeyValue("attemptNumber", job.attemptNumber()).log("Task execution claimed");

            // The durable claim makes recovery independent of this Kafka delivery's offset.
            // If a rebalance prevents the commit, finishing this claim is still safe; a redelivery
            // cannot obtain the same execution and will acknowledge it without running the handler.
            RuntimeException commitFailure = null;
            try {
                acknowledgment.acknowledge();
            } catch (RuntimeException failure) {
                commitFailure = failure;
                log.atWarn().addKeyValue("workflowId", job.workflowId()).addKeyValue("taskId", job.taskId())
                        .addKeyValue("executionId", executionId).addKeyValue("workerId", workerId)
                        .setCause(failure).log("Offset commit failed after durable claim; completing claimed execution");
            }

            Timer.Sample sample = Timer.start(metrics);
            TaskResult result = run(job);
            persistResult(result);
            sample.stop(Timer.builder("flowforge.worker.task.duration")
                    .tag("status", result.status()).publishPercentileHistogram().register(metrics));
            metrics.counter("flowforge.worker.task.results", "status", result.status()).increment();
            log.atInfo().addKeyValue("workflowId", job.workflowId()).addKeyValue("taskId", job.taskId())
                    .addKeyValue("executionId", executionId).addKeyValue("workerId", workerId)
                    .addKeyValue("status", result.status()).log("Task result persisted");
            if (commitFailure != null) {
                throw commitFailure;
            }
        } finally {
            if (claimed) {
                activeTasks.decrementAndGet();
            }
            slots.release();
        }
    }

    private TaskResult run(TaskJob job) {
        Future<Map<String, Object>> future = null;
        try {
            if (job.timeoutMs() < 1 || job.timeoutMs() > TimeUnit.DAYS.toMillis(1)) {
                throw new TaskExecutionException("Task timeout must be between 1 ms and 24 hours", false);
            }
            if (!store.isExecutionActive(job.executionId(), workerId)) {
                return result(job, "CANCELLED", false, Map.of(), "Execution cancelled before handler started");
            }
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(job.timeoutMs());
            future = executor.submit(() -> handlers.execute(job));
            while (true) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    return result(job, "TIMED_OUT", true, Map.of(), "Task exceeded timeout of " + job.timeoutMs() + " ms");
                }
                try {
                    Map<String, Object> output = future.get(
                            Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(cancellationPollMs)), TimeUnit.NANOSECONDS);
                    return result(job, "COMPLETED", false, output, null);
                } catch (TimeoutException pending) {
                    if (deadline - System.nanoTime() <= 0) {
                        return result(job, "TIMED_OUT", true, Map.of(), "Task exceeded timeout of " + job.timeoutMs() + " ms");
                    }
                    if (!accepting.get()) {
                        return result(job, "FAILED", true, Map.of(), "Worker shutdown interrupted task");
                    }
                    if (!store.isExecutionActive(job.executionId(), workerId)) {
                        return result(job, "CANCELLED", false, Map.of(), "Execution cancelled or superseded");
                    }
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return result(job, "FAILED", true, Map.of(), "Worker thread interrupted");
        } catch (ExecutionException executionFailure) {
            return failure(job, executionFailure.getCause());
        } catch (CancellationException cancelled) {
            return result(job, "FAILED", true, Map.of(), "Handler interrupted during worker shutdown");
        } catch (RuntimeException executionFailure) {
            return failure(job, executionFailure);
        } finally {
            if (future != null && !future.isDone()) {
                future.cancel(true);
                // Remove a cancelled queued future when every handler thread is still occupied.
                executor.remove((Runnable) future);
            }
        }
    }

    private TaskResult failure(TaskJob job, Throwable failure) {
        boolean retryable = failure instanceof TaskExecutionException taskFailure
                ? taskFailure.retryable() : !(failure instanceof IllegalArgumentException);
        log.atWarn().addKeyValue("workflowId", job.workflowId()).addKeyValue("taskId", job.taskId())
                .addKeyValue("executionId", job.executionId()).addKeyValue("workerId", workerId)
                .addKeyValue("retryable", retryable).setCause(failure).log("Task handler failed");
        String message = failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage();
        return result(job, "FAILED", retryable, Map.of(), message.substring(0, Math.min(message.length(), 2000)));
    }

    private TaskResult result(TaskJob job, String status, boolean retryable, Map<String, Object> output, String error) {
        return new TaskResult(job.workflowId(), job.taskId(), job.executionId(), workerId,
                status, retryable, output, error, Instant.now());
    }

    private void persistResult(TaskResult result) throws InterruptedException {
        for (int attempt = 1; ; attempt++) {
            try {
                store.recordResult(result);
                return;
            } catch (RuntimeException failure) {
                log.atError().addKeyValue("workflowId", result.workflowId()).addKeyValue("taskId", result.taskId())
                        .addKeyValue("executionId", result.executionId()).addKeyValue("workerId", workerId)
                        .addKeyValue("writeAttempt", attempt).setCause(failure).log("Task result persistence failed");
                if (attempt >= 3) {
                    // Keep the failure visible to Kafka's error handler. Persisted attempt deadlines
                    // recover this claim even while this process continues sending healthy heartbeats.
                    throw failure;
                }
                Thread.sleep(100L * attempt);
            }
        }
    }

    @PreDestroy
    public void close() {
        accepting.set(false);
        executor.shutdownNow();
    }
}
