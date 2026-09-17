package io.flowforge.shared;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.stream.Stream;

/**
 * PostgreSQL is the authority. Every lifecycle mutation locks workflow -> task; dispatch admission
 * additionally takes a transaction-scoped advisory lock. Kafka deliveries are notifications of
 * durable state, never permission to execute on their own.
 */
@Service
public class EngineStore {
    private static final Logger log = LoggerFactory.getLogger(EngineStore.class);
    private static final Set<String> TERMINAL = Set.of("COMPLETED", "FAILED", "CANCELLED");
    private final JdbcTemplate db;
    private final ObjectMapper json;
    private final int globalConcurrency;
    private final long workerTimeoutMs;
    private final Map<String,Integer> typeLimits;

    public EngineStore(JdbcTemplate db, ObjectMapper json,
            @Value("${flowforge.global-concurrency:32}") int globalConcurrency,
            @Value("${flowforge.worker-timeout-ms:15000}") long workerTimeoutMs,
            @Value("${flowforge.task-type-concurrency:}") String taskTypeConcurrency) {
        this.db = db; this.json = json; this.globalConcurrency = globalConcurrency; this.workerTimeoutMs = workerTimeoutMs;
        if (globalConcurrency < 1 || workerTimeoutMs < 1000) throw new IllegalArgumentException("Invalid engine limits");
        Map<String,Integer> limits = new HashMap<>();
        if (!taskTypeConcurrency.isBlank()) for (String entry : taskTypeConcurrency.split(",")) {
            String[] pair = entry.trim().split(":");
            if (pair.length != 2 || Integer.parseInt(pair[1]) < 1) throw new IllegalArgumentException("Use TYPE:limit for task-type-concurrency");
            limits.put(pair[0], Integer.parseInt(pair[1]));
        }
        this.typeLimits = Map.copyOf(limits);
    }

    @Transactional
    public Map<String,Object> submit(WorkflowDefinition definition) {
        DagValidator.validate(definition);
        if (definition.name() == null || definition.name().isBlank() || definition.name().length() > 200)
            throw new IllegalArgumentException("Workflow name is required and must be at most 200 characters");
        int limit = definition.concurrencyLimit() == null ? 8 : definition.concurrencyLimit();
        if (limit < 1 || limit > 1000) throw new IllegalArgumentException("concurrencyLimit must be 1..1000");
        UUID workflowId = UUID.randomUUID();
        db.update("INSERT INTO workflows(id,name,status,concurrency_limit) VALUES (?,?,'PENDING',?)",workflowId,definition.name(),limit);
        Map<String,UUID> ids = new LinkedHashMap<>();
        for (var task : definition.tasks()) {
            UUID id = UUID.randomUUID(); ids.put(task.name(),id);
            String payload = encode(task.payload() == null ? Map.of() : task.payload());
            if (payload.getBytes(StandardCharsets.UTF_8).length > 65536) throw new IllegalArgumentException("Task payload exceeds 64 KiB");
            db.update("""
                INSERT INTO workflow_tasks(id,workflow_id,name,task_type,payload,status,timeout_ms,max_retries,initial_retry_delay_ms,backoff_multiplier)
                VALUES (?,?,?,?,?::jsonb,'PENDING',?,?,?,?)
                """, id,workflowId,task.name(),task.taskType(),payload,task.effectiveTimeout(),task.effectiveRetries(),task.effectiveDelay(),task.effectiveBackoff());
        }
        for (var task : definition.tasks()) for (String parent : task.parents())
            db.update("INSERT INTO task_dependencies(workflow_id,task_id,parent_task_id) VALUES (?,?,?)",workflowId,ids.get(task.name()),ids.get(parent));
        event(workflowId,null,null,"WORKFLOW_SUBMITTED",Map.of("taskCount",ids.size(),"concurrencyLimit",limit));
        return workflow(workflowId);
    }

    public List<Map<String,Object>> workflows(int limit, int offset) {
        return rows(workflowSelect() + " ORDER BY w.created_at DESC LIMIT ? OFFSET ?",limit,offset);
    }
    public Map<String,Object> workflow(UUID id) {
        return required(rows(workflowSelect() + " WHERE w.id=?",id),"Workflow not found");
    }
    private String workflowSelect() {
        return """
            SELECT w.*,
            (SELECT count(*) FROM workflow_tasks t WHERE t.workflow_id=w.id) AS total_tasks,
            (SELECT count(*) FROM workflow_tasks t WHERE t.workflow_id=w.id AND t.status='COMPLETED') AS completed_tasks
            FROM workflows w
            """;
    }
    public List<Map<String,Object>> tasks(UUID workflowId) {
        workflow(workflowId);
        return rows("""
            SELECT t.*, coalesce((SELECT jsonb_agg(d.parent_task_id ORDER BY d.parent_task_id)
            FROM task_dependencies d WHERE d.task_id=t.id),'[]'::jsonb) AS dependencies
            FROM workflow_tasks t WHERE t.workflow_id=? ORDER BY t.created_at,t.name
            """, workflowId);
    }
    public List<Map<String,Object>> history(UUID workflowId,int limit,int offset) {
        workflow(workflowId);
        return rows("SELECT * FROM workflow_events WHERE workflow_id=? ORDER BY event_sequence LIMIT ? OFFSET ?",workflowId,limit,offset);
    }
    public List<Map<String,Object>> attempts(UUID workflowId) {
        workflow(workflowId);
        return rows("SELECT a.* FROM task_attempts a JOIN workflow_tasks t ON t.id=a.task_id WHERE t.workflow_id=? ORDER BY a.created_at,a.attempt_number",workflowId);
    }
    public List<Map<String,Object>> workers() {
        return rows("""
            SELECT id, CASE WHEN last_heartbeat < now() - (? * interval '1 millisecond') THEN 'UNHEALTHY' ELSE status END AS status,
            last_heartbeat, active_tasks, completed_tasks FROM workers ORDER BY last_heartbeat DESC LIMIT 500
            """,workerTimeoutMs);
    }
    public List<Map<String,Object>> deadLetters(int limit,int offset) {
        return rows("SELECT * FROM dead_letters ORDER BY created_at DESC LIMIT ? OFFSET ?",limit,offset);
    }
    public List<Map<String,Object>> payments(UUID workflowId) {
        return workflowId == null ? rows("SELECT * FROM payment_ledger ORDER BY created_at DESC LIMIT 500")
                : rows("SELECT * FROM payment_ledger WHERE workflow_id=? ORDER BY created_at",workflowId);
    }

    @Transactional
    public Map<String,Object> cancel(UUID workflowId) {
        Map<String,Object> workflow = lockWorkflow(workflowId);
        if (TERMINAL.contains(workflow.get("status"))) return workflow(workflowId);
        db.update("UPDATE workflows SET status='CANCELLED',finished_at=clock_timestamp() WHERE id=?",workflowId);
        cancelUnfinished(workflowId,"Workflow cancelled by request");
        event(workflowId,null,null,"WORKFLOW_CANCELLED",Map.of());
        return workflow(workflowId);
    }

    /** Single database statement per heartbeat; Redis cache is maintained separately and is non-authoritative. */
    public void heartbeat(UUID workerId,int activeTasks) {
        db.update("""
            INSERT INTO workers(id,status,last_heartbeat,active_tasks) VALUES (?,'HEALTHY',clock_timestamp(),?)
            ON CONFLICT(id) DO UPDATE SET status='HEALTHY',last_heartbeat=clock_timestamp(),active_tasks=excluded.active_tasks
            """,workerId,activeTasks);
    }
    public void markUnhealthyWorkers() {
        db.update("UPDATE workers SET status='UNHEALTHY',active_tasks=0 WHERE status='HEALTHY' AND last_heartbeat < now() - (? * interval '1 millisecond')",workerTimeoutMs);
    }

    @Transactional
    public Optional<TaskJob> claim(UUID executionId,UUID workerId) {
        List<Map<String,Object>> matches = rows("SELECT t.workflow_id,t.id FROM workflow_tasks t WHERE t.execution_id=?",executionId);
        if (matches.isEmpty()) return Optional.empty();
        UUID workflowId = uuid(matches.getFirst(),"workflowId");
        Map<String,Object> workflow = lockWorkflow(workflowId);
        if (TERMINAL.contains(workflow.get("status"))) return Optional.empty();
        Map<String,Object> task = lockTask(uuid(matches.getFirst(),"id"));
        if (!"READY".equals(task.get("status")) || !executionId.equals(task.get("executionId"))) return Optional.empty();
        // A successful claim proves liveness even before the next periodic heartbeat following an outage.
        db.update("""
            INSERT INTO workers(id,status,last_heartbeat) VALUES (?,'HEALTHY',clock_timestamp())
            ON CONFLICT(id) DO UPDATE SET status='HEALTHY',last_heartbeat=clock_timestamp()
            """,workerId);
        // now() is the transaction start and may predate a long wait for the workflow lock.
        // Sample the database clock after all claim locks, giving the handler its full timeout.
        Timestamp startedAt=Objects.requireNonNull(db.queryForObject("SELECT clock_timestamp()",Timestamp.class));
        db.update("UPDATE workflow_tasks SET status='RUNNING',worker_id=?,started_at=coalesce(started_at,?) WHERE id=?",workerId,startedAt,task.get("id"));
        db.update("UPDATE task_attempts SET status='RUNNING',worker_id=?,started_at=?,deadline=?::timestamptz + (? * interval '1 millisecond') WHERE execution_id=?",
                workerId,startedAt,startedAt,number(task,"timeoutMs"),executionId);
        event(workflowId,uuid(task,"id"),executionId,"TASK_STARTED",Map.of("workerId",workerId,"attemptNumber",task.get("attemptCount")));
        return Optional.of(job(task));
    }

    public boolean isExecutionActive(UUID executionId,UUID workerId) {
        return count("""
            SELECT count(*) FROM workflow_tasks t JOIN workflows w ON w.id=t.workflow_id
            WHERE t.execution_id=? AND t.worker_id=? AND t.status='RUNNING' AND w.status='RUNNING'
            """,executionId,workerId) == 1;
    }

    /** Result and its Kafka outbox message commit atomically. Repeated or stale reports are harmless. */
    @Transactional
    public void recordResult(TaskResult result) {
        if (!Set.of("COMPLETED","FAILED","TIMED_OUT","CANCELLED").contains(result.status()))
            throw new IllegalArgumentException("Invalid result status");
        Map<String,Object> workflow = lockWorkflow(result.workflowId());
        Map<String,Object> task = lockTask(result.taskId());
        if (TERMINAL.contains(workflow.get("status")) || !matches(task,result)) return;
        int updated = db.update("""
            UPDATE task_attempts SET result_payload=?::jsonb,result_received_at=clock_timestamp()
            WHERE execution_id=? AND status='RUNNING' AND result_received_at IS NULL
            """,encode(result),result.executionId());
        if (updated == 1) outbox(Topics.RESULTS,result.executionId(),result);
    }

    /** Kafka result consumers read the canonical persisted result, not arbitrary broker payload fields. */
    @Transactional
    public void applyResult(UUID executionId) {
        List<Map<String,Object>> results = rows("SELECT result_payload FROM task_attempts WHERE execution_id=? AND result_received_at IS NOT NULL",executionId);
        if (results.isEmpty()) return;
        TaskResult result = json.convertValue(results.getFirst().get("resultPayload"),TaskResult.class);
        Map<String,Object> workflow = lockWorkflow(result.workflowId());
        Map<String,Object> task = lockTask(result.taskId());
        if (TERMINAL.contains(workflow.get("status")) || !matches(task,result)) return;
        if ("COMPLETED".equals(result.status())) {
            db.update("UPDATE task_attempts SET status='COMPLETED',finished_at=clock_timestamp() WHERE execution_id=?",executionId);
            db.update("UPDATE workflow_tasks SET status='COMPLETED',finished_at=clock_timestamp(),result=?::jsonb WHERE id=?",encode(result.output()),result.taskId());
            db.update("UPDATE workers SET completed_tasks=completed_tasks+1 WHERE id=?",result.workerId());
            event(result.workflowId(),result.taskId(),executionId,"TASK_COMPLETED",Map.of("workerId",result.workerId()));
            finishIfComplete(result.workflowId());
        } else {
            failAttempt(task,"TIMED_OUT".equals(result.status()) ? "TIMED_OUT" : "FAILED",result.retryable(),
                    result.error() == null ? result.status() : result.error());
        }
    }
    private boolean matches(Map<String,Object> task,TaskResult result) {
        return "RUNNING".equals(task.get("status")) && result.workflowId().equals(task.get("workflowId"))
                && result.executionId().equals(task.get("executionId")) && result.workerId().equals(task.get("workerId"));
    }

    /**
     * The included database payment is fenced by workflow/task locks, the current attempt's
     * deadline, and absence of a durable result; its ledger row is unique across retries.
     * Interrupting a future alone cannot provide this guarantee for external side effects.
     */
    @Transactional
    public Map<String,Object> mockPayment(TaskJob job) {
        Map<String,Object> workflow = lockWorkflow(job.workflowId());
        Map<String,Object> task = lockTask(job.taskId());
        if (TERMINAL.contains(workflow.get("status")) || !job.workflowId().equals(task.get("workflowId"))
                || !job.executionId().equals(task.get("executionId")) || !"RUNNING".equals(task.get("status"))
                || count("""
                    SELECT count(*) FROM task_attempts WHERE execution_id=? AND status='RUNNING'
                    AND result_received_at IS NULL AND deadline>clock_timestamp()
                    """,job.executionId())!=1)
            throw new IllegalStateException("Execution is no longer active");
        Map<String,Object> payload = payload(task);
        BigDecimal amount;
        try { amount = new BigDecimal(String.valueOf(payload.get("amount"))).setScale(2,java.math.RoundingMode.UNNECESSARY); }
        catch (RuntimeException e) { throw new IllegalArgumentException("Payment amount must have at most two decimal places"); }
        if (amount.signum() <= 0 || amount.precision() > 20) throw new IllegalArgumentException("Payment amount must be positive and at most 20 digits");
        String currency = String.valueOf(payload.getOrDefault("currency","USD"));
        String orderId = String.valueOf(payload.getOrDefault("orderId",job.taskId().toString()));
        if (!currency.matches("[A-Z]{3}") || orderId.length()>200) throw new IllegalArgumentException("Invalid currency or order ID");
        db.update("""
            INSERT INTO payment_ledger(task_id,workflow_id,payment_id,amount,currency,order_id)
            VALUES (?,?,?,?,?,?) ON CONFLICT(task_id) DO NOTHING
            """,job.taskId(),job.workflowId(),UUID.randomUUID(),amount,currency,orderId);
        return required(rows("SELECT payment_id,amount,currency,order_id FROM payment_ledger WHERE task_id=?",job.taskId()),"Payment missing");
    }

    /** PostgreSQL advisory lock fences scheduler replicas even if the optional Redis lease expires. */
    @Transactional
    public int scheduleOnce() {
        if (!Boolean.TRUE.equals(db.queryForObject("SELECT pg_try_advisory_xact_lock(702198431)",Boolean.class))) return 0;
        // Recover all expired/lost executions first, including workflows outside the admission batch.
        List<Map<String,Object>> abandoned = rows("""
            SELECT DISTINCT t.workflow_id FROM workflow_tasks t JOIN task_attempts a ON a.execution_id=t.execution_id
            LEFT JOIN workers w ON w.id=t.worker_id
            WHERE t.status='RUNNING' AND a.result_received_at IS NULL
            AND (a.deadline < now() OR w.last_heartbeat < now() - (? * interval '1 millisecond'))
            ORDER BY t.workflow_id LIMIT 200
            """,workerTimeoutMs);
        for (Map<String,Object> row : abandoned) {
            UUID workflowId = uuid(row,"workflowId");
            Map<String,Object> workflow = lockWorkflow(workflowId);
            if (TERMINAL.contains(workflow.get("status"))) continue;
            for (Map<String,Object> task : rows("""
                SELECT t.*, (a.deadline < now()) AS expired FROM workflow_tasks t
                JOIN task_attempts a ON a.execution_id=t.execution_id LEFT JOIN workers w ON w.id=t.worker_id
                WHERE t.workflow_id=? AND t.status='RUNNING' AND a.result_received_at IS NULL
                AND (a.deadline < now() OR w.last_heartbeat < now() - (? * interval '1 millisecond'))
                """,workflowId,workerTimeoutMs)) {
                // A previous non-retryable sibling can have cancelled this task in this same transaction.
                if (!"RUNNING".equals(lockTask(uuid(task,"id")).get("status"))) continue;
                boolean expired = Boolean.TRUE.equals(task.get("expired"));
                failAttempt(task,expired ? "TIMED_OUT" : "FAILED",true,expired ? "Execution deadline exceeded" : "Worker heartbeat expired");
            }
        }
        int available = Math.max(0,globalConcurrency-(int)count("SELECT count(*) FROM workflow_tasks WHERE status IN ('READY','RUNNING')"));
        int dispatched=0;
        if (available == 0) return 0;
        // Types already at their cap cannot be admitted in this tick. Excluding them from
        // candidate selection keeps type-blocked workflows from consuming the bounded batch
        // below and starving younger workflows whose due work is of an uncapped type.
        List<String> saturatedTypes = typeLimits.entrySet().stream()
                .filter(limit -> count("SELECT count(*) FROM workflow_tasks WHERE task_type=? AND status IN ('READY','RUNNING')",limit.getKey())>=limit.getValue())
                .map(Map.Entry::getKey).sorted().toList();
        String dueTypeFilter = saturatedTypes.isEmpty() ? ""
                : " AND t.task_type NOT IN (" + String.join(",",Collections.nCopies(saturatedTypes.size(),"?")) + ")";
        Object[] saturatedArgs = saturatedTypes.toArray();
        // Only workflows with due, dependency-satisfied, admissible work compete for admission.
        List<Map<String,Object>> candidates = rows("""
            SELECT w.* FROM workflows w WHERE w.status IN ('PENDING','RUNNING')
            AND EXISTS (SELECT 1 FROM workflow_tasks t WHERE t.workflow_id=w.id AND t.status IN ('PENDING','RETRYING')
              AND t.next_run_at<=now()""" + dueTypeFilter + """
              AND NOT EXISTS (SELECT 1 FROM task_dependencies d JOIN workflow_tasks p ON p.id=d.parent_task_id
                WHERE d.task_id=t.id AND p.status<>'COMPLETED'))
            ORDER BY w.created_at LIMIT 200 FOR UPDATE OF w SKIP LOCKED
            """,saturatedArgs);
        for (Map<String,Object> workflow : candidates) {
            UUID workflowId=uuid(workflow,"id");
            int workflowSlots=(int)number(workflow,"concurrencyLimit")-(int)count("SELECT count(*) FROM workflow_tasks WHERE workflow_id=? AND status IN ('READY','RUNNING')",workflowId);
            if (workflowSlots<=0) continue;
            Object[] readyArgs = Stream.concat(Stream.of(workflowId),saturatedTypes.stream()).toArray();
            List<Map<String,Object>> ready=rows("""
                SELECT t.* FROM workflow_tasks t WHERE t.workflow_id=? AND t.status IN ('PENDING','RETRYING') AND t.next_run_at<=now()""" + dueTypeFilter + """
                AND NOT EXISTS (SELECT 1 FROM task_dependencies d JOIN workflow_tasks p ON p.id=d.parent_task_id
                    WHERE d.task_id=t.id AND p.status<>'COMPLETED') ORDER BY t.next_run_at,t.name LIMIT 1000
                """,readyArgs);
            for (Map<String,Object> task : ready) {
                String type=(String)task.get("taskType");
                if (typeLimits.containsKey(type) && count("SELECT count(*) FROM workflow_tasks WHERE task_type=? AND status IN ('READY','RUNNING')",type)>=typeLimits.get(type)) continue;
                if ("PENDING".equals(workflow.get("status"))) {
                    db.update("UPDATE workflows SET status='RUNNING',started_at=clock_timestamp() WHERE id=?",workflowId);
                    workflow.put("status","RUNNING"); event(workflowId,null,null,"WORKFLOW_STARTED",Map.of());
                }
                UUID executionId=UUID.randomUUID(); int attempt=(int)number(task,"attemptCount")+1;
                boolean retry="RETRYING".equals(task.get("status"));
                db.update("UPDATE workflow_tasks SET status='READY',execution_id=?,worker_id=NULL,attempt_count=? WHERE id=?",executionId,attempt,task.get("id"));
                db.update("INSERT INTO task_attempts(execution_id,task_id,attempt_number,status) VALUES (?,?,?,'READY')",executionId,task.get("id"),attempt);
                task.put("executionId",executionId);task.put("attemptCount",attempt);
                outbox(retry ? Topics.RETRY : Topics.READY,executionId,job(task));
                event(workflowId,uuid(task,"id"),executionId,"TASK_READY",Map.of("attemptNumber",attempt));
                dispatched++;available--;workflowSlots--;
                if (available==0) return dispatched;
                if (workflowSlots==0) break;
            }
        }
        return dispatched;
    }

    private void failAttempt(Map<String,Object> task,String status,boolean retryable,String error) {
        UUID workflowId=uuid(task,"workflowId"),taskId=uuid(task,"id"),executionId=uuid(task,"executionId");
        String reason=error.length()>4000 ? error.substring(0,4000) : error;
        db.update("UPDATE task_attempts SET status=?,finished_at=clock_timestamp(),error=? WHERE execution_id=?",status,reason,executionId);
        event(workflowId,taskId,executionId,"TASK_"+status,Map.of("error",reason,"retryable",retryable));
        if (RetryPolicy.shouldRetry((int)number(task,"attemptCount"),(int)number(task,"maxRetries"),retryable)) {
            long delay=RetryPolicy.delayMs(number(task,"initialRetryDelayMs"),((Number)task.get("backoffMultiplier")).doubleValue(),(int)number(task,"attemptCount"));
            db.update("UPDATE workflow_tasks SET status='RETRYING',worker_id=NULL,next_run_at=clock_timestamp() + (? * interval '1 millisecond') WHERE id=?",delay,taskId);
            event(workflowId,taskId,executionId,"TASK_RETRY_SCHEDULED",Map.of("delayMs",delay,"nextAttempt",number(task,"attemptCount")+1));
        } else {
            db.update("UPDATE workflow_tasks SET status=?,finished_at=clock_timestamp() WHERE id=?",status,taskId);
            Map<String,Object> deadLetter=Map.of("workflowId",workflowId,"taskId",taskId,"executionId",executionId,"reason",reason,"job",job(task),"createdAt",Instant.now());
            db.update("INSERT INTO dead_letters(id,workflow_id,task_id,execution_id,reason,payload) VALUES (?,?,?,?,?,?::jsonb) ON CONFLICT(execution_id) DO NOTHING",
                    UUID.randomUUID(),workflowId,taskId,executionId,reason,encode(deadLetter));
            outbox(Topics.DEAD_LETTER,executionId,deadLetter);
            event(workflowId,taskId,executionId,"TASK_DEAD_LETTERED",Map.of("reason",reason));
            db.update("UPDATE workflows SET status='FAILED',finished_at=clock_timestamp() WHERE id=?",workflowId);
            cancelUnfinished(workflowId,"Sibling task failed permanently");
            event(workflowId,null,null,"WORKFLOW_FAILED",Map.of("failedTaskId",taskId));
        }
    }
    private void finishIfComplete(UUID workflowId) {
        if (count("SELECT count(*) FROM workflow_tasks WHERE workflow_id=? AND status<>'COMPLETED'",workflowId)==0) {
            db.update("UPDATE workflows SET status='COMPLETED',finished_at=clock_timestamp() WHERE id=?",workflowId);
            event(workflowId,null,null,"WORKFLOW_COMPLETED",Map.of());
        }
    }
    private void cancelUnfinished(UUID workflowId,String reason) {
        for (Map<String,Object> task : rows("SELECT id,execution_id FROM workflow_tasks WHERE workflow_id=? AND status IN ('PENDING','READY','RUNNING','RETRYING')",workflowId)) {
            db.update("UPDATE task_attempts SET status='CANCELLED',finished_at=clock_timestamp(),error=? WHERE execution_id=? AND status IN ('READY','RUNNING')",reason,task.get("executionId"));
            event(workflowId,uuid(task,"id"),(UUID)task.get("executionId"),"TASK_CANCELLED",Map.of("reason",reason));
        }
        db.update("UPDATE workflow_tasks SET status='CANCELLED',finished_at=clock_timestamp() WHERE workflow_id=? AND status IN ('PENDING','READY','RUNNING','RETRYING')",workflowId);
    }

    public Map<String,Object> summary() {
        Map<String,Object> result=new LinkedHashMap<>();
        result.put("workflowsSubmitted",count("SELECT count(*) FROM workflows"));
        result.put("workflowsCompleted",count("SELECT count(*) FROM workflows WHERE status='COMPLETED'"));
        result.put("workflowsFailed",count("SELECT count(*) FROM workflows WHERE status='FAILED'"));
        result.put("tasksPerMinute",count("SELECT count(*) FROM workflow_tasks WHERE status='COMPLETED' AND finished_at>=now()-interval '1 minute'"));
        long completed=count("SELECT count(*) FROM task_attempts WHERE status='COMPLETED'");
        long failed=count("SELECT count(*) FROM task_attempts WHERE status IN ('FAILED','TIMED_OUT')");
        long total=count("SELECT count(*) FROM task_attempts");
        result.put("taskSuccessRate",completed+failed==0 ? 0d : (double)completed/(completed+failed));
        result.put("retryRate",total==0 ? 0d : (double)count("SELECT count(*) FROM task_attempts WHERE attempt_number>1")/total);
        result.put("deadLetterCount",count("SELECT count(*) FROM dead_letters"));
        result.put("queueDepth",count("SELECT count(*) FROM workflow_tasks WHERE status='READY'"));
        result.put("activeWorkers",count("SELECT count(*) FROM workers WHERE status='HEALTHY' AND last_heartbeat>=now() - (? * interval '1 millisecond')",workerTimeoutMs));
        result.putAll(required(rows("""
            SELECT coalesce(percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM (finished_at-started_at))*1000),0) AS p50_task_duration_ms,
            coalesce(percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM (finished_at-started_at))*1000),0) AS p95_task_duration_ms
            FROM task_attempts WHERE status='COMPLETED' AND started_at IS NOT NULL
            """),"Metrics unavailable"));
        result.put("pendingOutboxMessages",count("SELECT count(*) FROM outbox WHERE published_at IS NULL"));
        result.put("paymentSideEffects",count("SELECT count(*) FROM payment_ledger"));
        return result;
    }

    private Map<String,Object> lockWorkflow(UUID id) { return required(rows("SELECT * FROM workflows WHERE id=? FOR UPDATE",id),"Workflow not found"); }
    private Map<String,Object> lockTask(UUID id) { return required(rows("SELECT * FROM workflow_tasks WHERE id=? FOR UPDATE",id),"Task not found"); }
    private TaskJob job(Map<String,Object> task) {
        return new TaskJob(uuid(task,"workflowId"),uuid(task,"id"),uuid(task,"executionId"),(int)number(task,"attemptCount"),
                (String)task.get("taskType"),payload(task),number(task,"timeoutMs"),(int)number(task,"maxRetries"),
                number(task,"initialRetryDelayMs"),((Number)task.get("backoffMultiplier")).doubleValue(),Instant.now());
    }
    @SuppressWarnings("unchecked") private Map<String,Object> payload(Map<String,Object> task) { return (Map<String,Object>)task.get("payload"); }
    private void outbox(String topic,UUID key,Object body) {
        db.update("INSERT INTO outbox(id,topic,message_key,payload) VALUES (?,?,?,?::jsonb)",UUID.randomUUID(),topic,key.toString(),encode(body));
    }
    private void event(UUID workflowId,UUID taskId,UUID executionId,String type,Map<String,Object> details) {
        db.update("INSERT INTO workflow_events(id,workflow_id,task_id,execution_id,type,details) VALUES (?,?,?,?,?,?::jsonb)",
                UUID.randomUUID(),workflowId,taskId,executionId,type,encode(details));
        log.atInfo().addKeyValue("workflowId",workflowId).addKeyValue("taskId",taskId).addKeyValue("executionId",executionId)
                .addKeyValue("workerId",details.get("workerId")).addKeyValue("event",type).log("Workflow state changed");
    }
    String encode(Object value) {
        try { return json.writeValueAsString(value == null ? Map.of() : value); }
        catch (Exception e) { throw new IllegalArgumentException("Cannot serialize JSON",e); }
    }
    List<Map<String,Object>> rows(String sql,Object... args) { return db.query(sql,this::mapRow,args); }
    private Map<String,Object> mapRow(ResultSet rs,int rowNumber) throws SQLException {
        Map<String,Object> row=new LinkedHashMap<>();
        for (int i=1;i<=rs.getMetaData().getColumnCount();i++) {
            String column=rs.getMetaData().getColumnLabel(i); Object value=rs.getObject(i);
            if (value instanceof Timestamp timestamp) value=timestamp.toInstant();
            if (value != null && (rs.getMetaData().getColumnTypeName(i).equals("jsonb") || rs.getMetaData().getColumnTypeName(i).equals("json"))) {
                try { value=json.readValue(value.toString(),new TypeReference<Object>(){}); }
                catch (Exception e) { throw new SQLException("Invalid stored JSON",e); }
            }
            row.put(camel(column),value);
        }
        return row;
    }
    private String camel(String name) {
        StringBuilder value=new StringBuilder(); boolean upper=false;
        for (char c : name.toCharArray()) { if (c=='_') upper=true; else { value.append(upper ? Character.toUpperCase(c) : c);upper=false; } }
        return value.toString();
    }
    private long count(String sql,Object... args) { return Objects.requireNonNull(db.queryForObject(sql,Long.class,args)); }
    private static UUID uuid(Map<String,Object> row,String key) { return (UUID)row.get(key); }
    private static long number(Map<String,Object> row,String key) { return ((Number)row.get(key)).longValue(); }
    private static Map<String,Object> required(List<Map<String,Object>> rows,String message) {
        if (rows.isEmpty()) throw new MissingResourceException(message,EngineStore.class.getName(),"");
        return rows.getFirst();
    }
}
