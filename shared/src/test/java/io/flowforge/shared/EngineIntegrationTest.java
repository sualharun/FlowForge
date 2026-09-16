package io.flowforge.shared;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import static org.assertj.core.api.Assertions.*;

/** Real PostgreSQL constraints/transactions and a real Kafka broker; no H2 substitutes. */
@Testcontainers
@SpringBootTest(classes=EngineIntegrationTest.TestApp.class,properties={
        "spring.config.import=classpath:flowforge-common.yml","flowforge.global-concurrency=3", "flowforge.topic-partitions=2"})
class EngineIntegrationTest {
    @Container static final PostgreSQLContainer<?> postgres=new PostgreSQLContainer<>("postgres:16-alpine");
    @Container static final KafkaContainer kafka=new KafkaContainer("apache/kafka-native:3.9.1");
    @DynamicPropertySource static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url",postgres::getJdbcUrl);
        registry.add("spring.datasource.username",postgres::getUsername);
        registry.add("spring.datasource.password",postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers",kafka::getBootstrapServers);
    }
    @SpringBootConfiguration @EnableAutoConfiguration
    @Import({EngineStore.class,KafkaConfiguration.class,OutboxPublisher.class})
    static class TestApp {}
    @Autowired EngineStore store;
    @Autowired JdbcTemplate db;
    @Autowired OutboxPublisher publisher;
    @Autowired ObjectMapper json;
    UUID worker;
    @BeforeEach void clear() {
        db.execute("TRUNCATE payment_ledger,dead_letters,outbox,workflow_events,task_dependencies,task_attempts,workflow_tasks,workers,workflows CASCADE");
        worker=UUID.randomUUID();store.heartbeat(worker,0);
    }
    WorkflowDefinition.TaskDefinition task(String name,String type,int retries,String... parents) {
        return new WorkflowDefinition.TaskDefinition(name,type,type.equals("MOCK_PAYMENT") ? Map.of("amount",25,"currency","USD") : Map.of("durationMs",1),List.of(parents),10000L,retries,1000L,2d);
    }
    UUID submit(int concurrency,WorkflowDefinition.TaskDefinition... tasks) {
        return (UUID)store.submit(new WorkflowDefinition("test",concurrency,List.of(tasks))).get("id");
    }
    Map<String,Object> task(UUID workflow,String name) { return store.tasks(workflow).stream().filter(t->t.get("name").equals(name)).findFirst().orElseThrow(); }
    TaskJob claim(UUID workflow,String name) { return store.claim((UUID)task(workflow,name).get("executionId"),worker).orElseThrow(); }
    void result(TaskJob job,String status,boolean retryable) {
        store.recordResult(new TaskResult(job.workflowId(),job.taskId(),job.executionId(),worker,status,retryable,Map.of("ok",true),status.equals("COMPLETED")?null:"simulated failure",Instant.now()));
        store.applyResult(job.executionId());
    }
    void due(UUID workflow) { db.update("UPDATE workflow_tasks SET next_run_at=now()-interval '1 second' WHERE workflow_id=?",workflow); }

    @Test void diamondWaitsForBothParentsAndCompletes() {
        UUID id=submit(3,task("a","DELAY",0),task("b","DELAY",0,"a"),task("c","DELAY",0,"a"),task("d","DELAY",0,"b","c"));
        assertThat(store.scheduleOnce()).isEqualTo(1);result(claim(id,"a"),"COMPLETED",false);
        assertThat(store.scheduleOnce()).isEqualTo(2);
        TaskJob b=claim(id,"b"),c=claim(id,"c");result(b,"COMPLETED",false);
        store.scheduleOnce();assertThat(task(id,"d").get("status")).isEqualTo("PENDING");
        result(c,"COMPLETED",false);assertThat(store.scheduleOnce()).isEqualTo(1);
        result(claim(id,"d"),"COMPLETED",false);
        assertThat(store.workflow(id).get("status")).isEqualTo("COMPLETED");
        assertThat(store.history(id,100,0)).extracting(e->e.get("type")).contains("WORKFLOW_COMPLETED");
    }

    @Test void concurrentDuplicateClaimsAndPaymentsProduceOneSideEffect() throws Exception {
        UUID id=submit(1,task("pay","MOCK_PAYMENT",2));store.scheduleOnce();UUID execution=(UUID)task(id,"pay").get("executionId");
        var start=new CountDownLatch(1);var jobs=new ArrayList<Future<Optional<TaskJob>>>();
        try(var pool=Executors.newVirtualThreadPerTaskExecutor()) {
            for(int i=0;i<16;i++) jobs.add(pool.submit(()->{start.await();return store.claim(execution,worker);}));
            start.countDown();int winners=0;TaskJob claimed=null;
            for(var future:jobs) { var value=future.get(15,TimeUnit.SECONDS);if(value.isPresent()){winners++;claimed=value.get();} }
            assertThat(winners).isEqualTo(1);
            assertThat(store.mockPayment(claimed)).isEqualTo(store.mockPayment(claimed));
            result(claimed,"COMPLETED",false);result(claimed,"COMPLETED",false);
        }
        assertThat(store.payments(id)).hasSize(1);
        assertThat(db.queryForObject("SELECT count(*) FROM outbox WHERE topic=?",Integer.class,Topics.RESULTS)).isEqualTo(1);
        assertThat(store.workers().getFirst().get("completedTasks")).isEqualTo(1L);
    }

    @Test void retriesWithExponentialBackoffThenDeadLettersAndCancelsDescendants() {
        UUID id=submit(1,task("fail","DELAY",2),task("child","DELAY",0,"fail"));
        for(int i=1;i<=3;i++) {
            due(id);store.scheduleOnce();TaskJob job=claim(id,"fail");assertThat(job.attemptNumber()).isEqualTo(i);
            result(job,"FAILED",true);
            if(i<3) { assertThat(task(id,"fail").get("status")).isEqualTo("RETRYING");assertThat(store.scheduleOnce()).isZero(); }
        }
        assertThat(store.workflow(id).get("status")).isEqualTo("FAILED");
        assertThat(task(id,"child").get("status")).isEqualTo("CANCELLED");
        assertThat(store.deadLetters(100,0)).hasSize(1);
        assertThat(db.queryForList("SELECT (details->>'delayMs')::bigint AS delay FROM workflow_events WHERE type='TASK_RETRY_SCHEDULED' ORDER BY created_at",Long.class)).containsExactly(1000L,2000L);
        assertThat(db.queryForObject("SELECT count(*) FROM outbox WHERE topic=?",Integer.class,Topics.RETRY)).isEqualTo(2);
        assertThat(db.queryForObject("SELECT count(*) FROM outbox WHERE topic=?",Integer.class,Topics.DEAD_LETTER)).isEqualTo(1);
    }

    @Test void nonRetryableFailureSkipsRemainingRetries() {
        UUID id=submit(1,task("fail","DELAY",10));store.scheduleOnce();result(claim(id,"fail"),"FAILED",false);
        assertThat(store.workflow(id).get("status")).isEqualTo("FAILED");assertThat(store.attempts(id)).hasSize(1);
    }

    @Test void independentDeadlineRecoveryWorksWithHealthyWorker() {
        UUID id=submit(1,task("slow","DELAY",1));store.scheduleOnce();TaskJob old=claim(id,"slow");
        db.update("UPDATE task_attempts SET deadline=now()-interval '1 second' WHERE execution_id=?",old.executionId());
        store.scheduleOnce();assertThat(task(id,"slow").get("status")).isEqualTo("RETRYING");
        assertThat(store.attempts(id).getFirst().get("status")).isEqualTo("TIMED_OUT");
        assertThat(store.workers().getFirst().get("status")).isEqualTo("HEALTHY");
    }

    @Test void expiredAttemptCannotWritePaymentBeforeSchedulerProcessesTimeout() {
        UUID id=submit(1,task("pay","MOCK_PAYMENT",1));store.scheduleOnce();TaskJob expired=claim(id,"pay");
        db.update("UPDATE task_attempts SET deadline=clock_timestamp()-interval '1 second' WHERE execution_id=?",expired.executionId());
        assertThat(task(id,"pay").get("status")).isEqualTo("RUNNING");
        assertThatThrownBy(()->store.mockPayment(expired)).isInstanceOf(IllegalStateException.class);
        assertThat(store.payments(id)).isEmpty();
    }

    @Test void durableResultBlocksNewPaymentBeforeKafkaAppliesResult() {
        UUID id=submit(1,task("pay","MOCK_PAYMENT",1));store.scheduleOnce();TaskJob completed=claim(id,"pay");
        store.recordResult(new TaskResult(id,completed.taskId(),completed.executionId(),worker,"TIMED_OUT",true,Map.of(),"timeout",Instant.now()));
        assertThat(task(id,"pay").get("status")).isEqualTo("RUNNING");
        assertThatThrownBy(()->store.mockPayment(completed)).isInstanceOf(IllegalStateException.class);
        assertThat(store.payments(id)).isEmpty();
    }

    @Test void workerFailureFencesOldAttemptAndRecoversOnAnotherWorker() {
        UUID id=submit(1,task("pay","MOCK_PAYMENT",2));store.scheduleOnce();TaskJob old=claim(id,"pay");store.mockPayment(old);
        db.update("UPDATE workers SET last_heartbeat=now()-interval '1 minute' WHERE id=?",worker);
        store.markUnhealthyWorkers();store.scheduleOnce();assertThat(store.isExecutionActive(old.executionId(),worker)).isFalse();
        assertThatThrownBy(()->store.mockPayment(old)).isInstanceOf(IllegalStateException.class);
        result(old,"COMPLETED",false);assertThat(task(id,"pay").get("status")).isEqualTo("RETRYING");
        worker=UUID.randomUUID();store.heartbeat(worker,0);due(id);store.scheduleOnce();TaskJob retry=claim(id,"pay");
        store.mockPayment(retry);result(retry,"COMPLETED",false);
        assertThat(store.payments(id)).hasSize(1);assertThat(store.workflow(id).get("status")).isEqualTo("COMPLETED");
    }

    @Test void cancellationStopsPendingAndRunningWorkAndRejectsLateResult() {
        UUID id=submit(2,task("a","MOCK_PAYMENT",1),task("b","DELAY",1,"a"));store.scheduleOnce();TaskJob running=claim(id,"a");
        store.cancel(id);store.cancel(id);
        assertThat(store.tasks(id)).allMatch(t->t.get("status").equals("CANCELLED"));
        assertThatThrownBy(()->store.mockPayment(running)).isInstanceOf(IllegalStateException.class);
        result(running,"COMPLETED",false);assertThat(store.scheduleOnce()).isZero();
        assertThat(store.workflow(id).get("status")).isEqualTo("CANCELLED");assertThat(store.payments(id)).isEmpty();
    }

    @Test void persistedResultIsNotTimedOutWhileKafkaIsUnavailable() {
        UUID id=submit(1,task("a","DELAY",2));store.scheduleOnce();TaskJob job=claim(id,"a");
        store.recordResult(new TaskResult(id,job.taskId(),job.executionId(),worker,"COMPLETED",false,Map.of(),null,Instant.now()));
        db.update("UPDATE task_attempts SET deadline=now()-interval '1 minute' WHERE execution_id=?",job.executionId());
        store.scheduleOnce();assertThat(task(id,"a").get("status")).isEqualTo("RUNNING");
        store.applyResult(job.executionId());assertThat(store.workflow(id).get("status")).isEqualTo("COMPLETED");
    }

    @Test void concurrentSchedulersHonorGlobalAndWorkflowConcurrency() throws Exception {
        UUID first=submit(1,task("a","DELAY",0),task("b","DELAY",0));
        UUID second=submit(3,task("a","DELAY",0),task("b","DELAY",0),task("c","DELAY",0));
        try(var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var calls=new ArrayList<Future<Integer>>();for(int i=0;i<8;i++) calls.add(executor.submit(store::scheduleOnce));
            for(var call:calls)call.get(10,TimeUnit.SECONDS);
        }
        assertThat(store.tasks(first).stream().filter(t->t.get("status").equals("READY")).count()).isEqualTo(1);
        assertThat(store.tasks(second).stream().filter(t->t.get("status").equals("READY")).count()).isEqualTo(2);
    }

    @Test void typeConcurrencyIsAlsoDurableAcrossSchedulerRuns() {
        EngineStore limited=new EngineStore(db,json,3,15000,"MOCK_PAYMENT:1");
        // Programmatic transaction is necessary because this purpose-specific store is not a proxied Spring bean.
        var tx=new org.springframework.transaction.support.TransactionTemplate(new org.springframework.jdbc.datasource.DataSourceTransactionManager(db.getDataSource()));
        UUID id=submit(3,task("a","MOCK_PAYMENT",0),task("b","MOCK_PAYMENT",0),task("c","DELAY",0));
        tx.executeWithoutResult(s->limited.scheduleOnce());
        assertThat(store.tasks(id).stream().filter(t->t.get("taskType").equals("MOCK_PAYMENT")&&t.get("status").equals("READY")).count()).isEqualTo(1);
        assertThat(task(id,"c").get("status")).isEqualTo("READY");
    }

    @Test void saturatedTaskTypeDoesNotHideAnotherRunnableTypeBehindAdmissionLimit() {
        EngineStore limited=new EngineStore(db,json,3,15000,"MOCK_PAYMENT:1");
        var tx=new org.springframework.transaction.support.TransactionTemplate(new org.springframework.jdbc.datasource.DataSourceTransactionManager(db.getDataSource()));
        UUID occupied=submit(1,task("payment","MOCK_PAYMENT",0));
        tx.executeWithoutResult(s->limited.scheduleOnce());
        assertThat(task(occupied,"payment").get("status")).isEqualTo("READY");

        UUID waiting=submit(1,task("a-payment","MOCK_PAYMENT",0),task("b-delay","DELAY",0),task("c-delay","DELAY",0));
        tx.executeWithoutResult(s->limited.scheduleOnce());
        assertThat(task(waiting,"a-payment").get("status")).isEqualTo("PENDING");
        assertThat(task(waiting,"b-delay").get("status")).isEqualTo("READY");
        // Scanning past a blocked type must still enforce the workflow's single admission slot.
        assertThat(task(waiting,"c-delay").get("status")).isEqualTo("PENDING");
    }

    @Test void typeBlockedOlderWorkflowsDoNotStarveANewerWorkflowPastTheCandidateBatch() {
        EngineStore limited=new EngineStore(db,json,64,15000,"MOCK_PAYMENT:1");
        var tx=new org.springframework.transaction.support.TransactionTemplate(new org.springframework.jdbc.datasource.DataSourceTransactionManager(db.getDataSource()));
        UUID saturating=submit(1,task("payment","MOCK_PAYMENT",0));
        tx.executeWithoutResult(s->limited.scheduleOnce());
        assertThat(task(saturating,"payment").get("status")).isEqualTo("READY");
        // Exactly fill the 200-row admission batch with older workflows whose only due work
        // is of the now-saturated type. They can never be admitted in this tick.
        for(int i=0;i<200;i++) submit(1,task("blocked","MOCK_PAYMENT",0));
        UUID newest=submit(1,task("runnable","DELAY",0));

        tx.executeWithoutResult(s->limited.scheduleOnce());

        assertThat(task(newest,"runnable").get("status")).isEqualTo("READY");
        assertThat(db.queryForObject("SELECT count(*) FROM workflow_tasks WHERE task_type='MOCK_PAYMENT' AND status='READY'",Long.class)).isEqualTo(1L);
    }

    @Test void successfulClaimRenewsExpiredWorkerHeartbeatWithoutChangingActiveCount() {
        UUID id=submit(1,task("a","DELAY",0));store.scheduleOnce();
        db.update("UPDATE workers SET status='UNHEALTHY',last_heartbeat=now()-interval '1 minute',active_tasks=2 WHERE id=?",worker);
        TaskJob claimed=claim(id,"a");
        assertThat(store.workers().getFirst()).containsEntry("status","HEALTHY").containsEntry("activeTasks",2);
        store.scheduleOnce();
        assertThat(store.isExecutionActive(claimed.executionId(),worker)).isTrue();
        assertThat(task(id,"a").get("status")).isEqualTo("RUNNING");
    }

    @Test void claimTimeoutAndEventTimestampStartAfterWaitingForWorkflowLock() throws Exception {
        var quick=new WorkflowDefinition.TaskDefinition("quick","DELAY",Map.of("durationMs",1),List.of(),500L,0,1000L,2d);
        UUID id=submit(1,quick);store.scheduleOnce();
        UUID execution=(UUID)task(id,"quick").get("executionId");
        var tx=new org.springframework.transaction.support.TransactionTemplate(new org.springframework.jdbc.datasource.DataSourceTransactionManager(db.getDataSource()));
        var claimantPid=new CompletableFuture<Integer>();
        try(var blocker=Objects.requireNonNull(db.getDataSource()).getConnection();var callers=Executors.newVirtualThreadPerTaskExecutor()) {
            blocker.setAutoCommit(false);
            try(var statement=blocker.prepareStatement("SELECT id FROM workflows WHERE id=? FOR UPDATE")) {
                statement.setObject(1,id);
                try(var rows=statement.executeQuery()) { assertThat(rows.next()).isTrue(); }
            }
            try {
                Future<Optional<TaskJob>> claim=callers.submit(()->tx.execute(status->{
                    claimantPid.complete(db.queryForObject("SELECT pg_backend_pid()",Integer.class));
                    return store.claim(execution,worker);
                }));
                int pid=claimantPid.get(5,TimeUnit.SECONDS);
                long waitDeadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
                boolean waiting=false;
                while(System.nanoTime()<waitDeadline) {
                    waiting=Boolean.TRUE.equals(db.queryForObject("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=? AND wait_event_type='Lock')",Boolean.class,pid));
                    if(waiting) break;
                    Thread.sleep(10);
                }
                assertThat(waiting).as("claim must actually be blocked on the workflow lock").isTrue();
                Thread.sleep(700); // More than the task's 500 ms timeout, before execution can begin.
                Instant releasedAt=Objects.requireNonNull(db.queryForObject("SELECT clock_timestamp()",Timestamp.class)).toInstant();
                blocker.commit();
                assertThat(claim.get(5,TimeUnit.SECONDS)).isPresent();
                var attempt=store.attempts(id).getFirst();
                Instant startedAt=(Instant)attempt.get("startedAt");
                assertThat(startedAt).isAfterOrEqualTo(releasedAt);
                assertThat((Instant)attempt.get("deadline")).isEqualTo(startedAt.plusMillis(500));
                var startedEvent=store.history(id,100,0).stream().filter(event->event.get("type").equals("TASK_STARTED")).findFirst().orElseThrow();
                assertThat((Instant)startedEvent.get("createdAt")).isAfterOrEqualTo(releasedAt);
            } finally {
                // Release before executor close so a failed assertion cannot strand a blocked claim.
                blocker.rollback();
            }
        }
    }

    @Test void invalidPayloadRollsBackEntireSubmission() {
        var invalid=new WorkflowDefinition.TaskDefinition("large","DELAY",Map.of("value","x".repeat(70000)),List.of(),1000L,0,1000L,2d);
        assertThatThrownBy(()->submit(1,task("first","DELAY",0),invalid)).hasMessageContaining("64 KiB");
        assertThat(store.workflows(100,0)).isEmpty();
    }

    @Test void repositoryRejectsCrossWorkflowDependency() {
        UUID first=submit(1,task("first","DELAY",0));UUID second=submit(1,task("second","DELAY",0));
        assertThatThrownBy(()->db.update("INSERT INTO task_dependencies(workflow_id,task_id,parent_task_id) VALUES (?,?,?)",first,task(first,"first").get("id"),task(second,"second").get("id")))
                .isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
    }

    @Test void outboxSurvivesReplayAndKafkaCarriesDispatchResultsAndDeadLetters() throws Exception {
        UUID id=submit(1,task("pay","MOCK_PAYMENT",0));store.scheduleOnce();
        Properties config=new Properties();config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafka.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG,"test-"+UUID.randomUUID());config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        try(var consumer=new KafkaConsumer<String,String>(config)) {
            consumer.subscribe(List.of(Topics.READY,Topics.RESULTS,Topics.DEAD_LETTER));publisher.publish();
            UUID execution=(UUID)task(id,"pay").get("executionId");TaskJob received=null;
            long deadline=System.nanoTime()+Duration.ofSeconds(20).toNanos();
            while(received==null && System.nanoTime()<deadline) for(var record:consumer.poll(Duration.ofMillis(250)))
                if(record.topic().equals(Topics.READY)) { TaskJob job=json.readValue(record.value(),TaskJob.class);if(job.executionId().equals(execution))received=job; }
            assertThat(received).isNotNull();TaskJob claimed=store.claim(received.executionId(),worker).orElseThrow();store.mockPayment(claimed);
            // Simulate a relay crash after broker ACK but before published_at commit.
            db.update("UPDATE outbox SET published_at=NULL WHERE topic=?",Topics.READY);publisher.publish();
            assertThat(store.claim(execution,worker)).isEmpty();assertThat(store.payments(id)).hasSize(1);
            result(claimed,"FAILED",false);publisher.publish();Set<String> observed=new HashSet<>();
            deadline=System.nanoTime()+Duration.ofSeconds(20).toNanos();
            while(observed.size()<2 && System.nanoTime()<deadline)for(var record:consumer.poll(Duration.ofMillis(250))) {
                if(record.key().equals(execution.toString()) && (record.topic().equals(Topics.RESULTS)||record.topic().equals(Topics.DEAD_LETTER)))observed.add(record.topic());
            }
            assertThat(observed).containsExactlyInAnyOrder(Topics.RESULTS,Topics.DEAD_LETTER);
        }
        assertThat(db.queryForObject("SELECT count(*) FROM outbox WHERE published_at IS NULL",Integer.class)).isZero();
    }
}
