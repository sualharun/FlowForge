-- PostgreSQL now() remains fixed for the entire transaction, including lock waits.
-- Record the time each row is actually created so history reflects execution order.
ALTER TABLE workflows ALTER COLUMN created_at SET DEFAULT clock_timestamp();
ALTER TABLE workflow_tasks ALTER COLUMN created_at SET DEFAULT clock_timestamp();
ALTER TABLE workflow_tasks ALTER COLUMN next_run_at SET DEFAULT clock_timestamp();
ALTER TABLE task_attempts ALTER COLUMN created_at SET DEFAULT clock_timestamp();
ALTER TABLE workflow_events ALTER COLUMN created_at SET DEFAULT clock_timestamp();
ALTER TABLE outbox ALTER COLUMN created_at SET DEFAULT clock_timestamp();
ALTER TABLE dead_letters ALTER COLUMN created_at SET DEFAULT clock_timestamp();
ALTER TABLE payment_ledger ALTER COLUMN created_at SET DEFAULT clock_timestamp();
