CREATE TABLE workflows (
 id uuid PRIMARY KEY, name varchar(200) NOT NULL,
 status varchar(20) NOT NULL CHECK (status IN ('PENDING','RUNNING','COMPLETED','FAILED','CANCELLED')),
 concurrency_limit integer NOT NULL CHECK (concurrency_limit BETWEEN 1 AND 1000),
 created_at timestamptz NOT NULL DEFAULT now(), started_at timestamptz, finished_at timestamptz
);
CREATE INDEX workflows_status ON workflows(status,created_at);
CREATE TABLE workers (
 id uuid PRIMARY KEY, status varchar(20) NOT NULL CHECK(status IN ('HEALTHY','UNHEALTHY')),
 last_heartbeat timestamptz NOT NULL, active_tasks integer NOT NULL DEFAULT 0,
 completed_tasks bigint NOT NULL DEFAULT 0
);
CREATE INDEX workers_heartbeat ON workers(last_heartbeat);
CREATE TABLE workflow_tasks (
 id uuid PRIMARY KEY, workflow_id uuid NOT NULL REFERENCES workflows(id), name varchar(200) NOT NULL,
 task_type varchar(40) NOT NULL, payload jsonb NOT NULL, result jsonb,
 status varchar(20) NOT NULL CHECK(status IN ('PENDING','READY','RUNNING','RETRYING','COMPLETED','FAILED','CANCELLED','TIMED_OUT')),
 timeout_ms bigint NOT NULL CHECK(timeout_ms BETWEEN 10 AND 3600000),
 max_retries integer NOT NULL CHECK(max_retries BETWEEN 0 AND 20), initial_retry_delay_ms bigint NOT NULL,
 backoff_multiplier double precision NOT NULL, attempt_count integer NOT NULL DEFAULT 0,
 execution_id uuid, worker_id uuid REFERENCES workers(id), next_run_at timestamptz NOT NULL DEFAULT now(),
 created_at timestamptz NOT NULL DEFAULT now(), started_at timestamptz, finished_at timestamptz,
 UNIQUE(workflow_id,name), UNIQUE(workflow_id,id)
);
CREATE INDEX tasks_workflow ON workflow_tasks(workflow_id,status);
CREATE INDEX tasks_ready ON workflow_tasks(next_run_at) WHERE status IN ('PENDING','READY','RETRYING');
CREATE INDEX tasks_running ON workflow_tasks(worker_id) WHERE status='RUNNING';
CREATE UNIQUE INDEX tasks_execution ON workflow_tasks(execution_id) WHERE execution_id IS NOT NULL;
CREATE TABLE task_dependencies (
 workflow_id uuid NOT NULL REFERENCES workflows(id), task_id uuid NOT NULL, parent_task_id uuid NOT NULL,
 PRIMARY KEY(task_id,parent_task_id), CHECK(task_id<>parent_task_id),
 FOREIGN KEY(workflow_id,task_id) REFERENCES workflow_tasks(workflow_id,id),
 FOREIGN KEY(workflow_id,parent_task_id) REFERENCES workflow_tasks(workflow_id,id)
);
CREATE INDEX dependencies_parent ON task_dependencies(parent_task_id);
CREATE TABLE task_attempts (
 execution_id uuid PRIMARY KEY, task_id uuid NOT NULL REFERENCES workflow_tasks(id),
 attempt_number integer NOT NULL, worker_id uuid REFERENCES workers(id),
 status varchar(20) NOT NULL CHECK(status IN ('READY','RUNNING','COMPLETED','FAILED','TIMED_OUT','CANCELLED')),
 created_at timestamptz NOT NULL DEFAULT now(), started_at timestamptz, finished_at timestamptz,
 deadline timestamptz, error text, result_payload jsonb, result_received_at timestamptz,
 UNIQUE(task_id,attempt_number)
);
CREATE INDEX attempts_deadline ON task_attempts(deadline) WHERE status='RUNNING';
CREATE TABLE workflow_events (
 id uuid PRIMARY KEY, event_sequence bigint GENERATED ALWAYS AS IDENTITY UNIQUE, workflow_id uuid NOT NULL REFERENCES workflows(id), task_id uuid REFERENCES workflow_tasks(id),
 execution_id uuid, type varchar(80) NOT NULL, details jsonb NOT NULL DEFAULT '{}', created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX events_workflow ON workflow_events(workflow_id,event_sequence);
CREATE TABLE outbox (
 id uuid PRIMARY KEY, topic varchar(100) NOT NULL, message_key varchar(100) NOT NULL,
 payload jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), published_at timestamptz
);
CREATE INDEX outbox_unpublished ON outbox(created_at) WHERE published_at IS NULL;
CREATE TABLE dead_letters (
 id uuid PRIMARY KEY, workflow_id uuid NOT NULL REFERENCES workflows(id), task_id uuid NOT NULL REFERENCES workflow_tasks(id),
 execution_id uuid NOT NULL UNIQUE REFERENCES task_attempts(execution_id), reason text NOT NULL,
 payload jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE payment_ledger (
 task_id uuid PRIMARY KEY REFERENCES workflow_tasks(id), workflow_id uuid NOT NULL REFERENCES workflows(id),
 payment_id uuid NOT NULL UNIQUE, amount numeric(20,2) NOT NULL CHECK(amount>0),
 currency varchar(3) NOT NULL, order_id varchar(200) NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
