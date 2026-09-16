export interface Workflow {
  id: string;
  name: string;
  status: string;
  createdAt: string;
  startedAt: string | null;
  finishedAt: string | null;
  concurrencyLimit: number;
  totalTasks: number;
  completedTasks: number;
}

export interface Task {
  id: string;
  workflowId: string;
  name: string;
  taskType: string;
  status: string;
  payload: unknown;
  result: unknown;
  timeoutMs: number;
  maxRetries: number;
  initialRetryDelayMs: number;
  backoffMultiplier: number;
  attemptCount: number;
  executionId: string | null;
  workerId: string | null;
  createdAt: string;
  startedAt: string | null;
  finishedAt: string | null;
  dependencies: string[];
}

export interface WorkflowEvent {
  id: string;
  workflowId: string;
  taskId: string | null;
  executionId: string | null;
  type: string;
  details: unknown;
  createdAt: string;
}

export interface Worker {
  id: string;
  status: string;
  lastHeartbeat: string;
  activeTasks: number;
  completedTasks: number;
}

export interface DeadLetter {
  id: string;
  workflowId: string;
  taskId: string;
  executionId: string;
  reason: string;
  createdAt: string;
  payload: unknown;
}

export interface Metrics {
  workflowsSubmitted: number;
  workflowsCompleted: number;
  workflowsFailed: number;
  tasksPerMinute: number;
  taskSuccessRate: number;
  retryRate: number;
  deadLetterCount: number;
  queueDepth: number;
  activeWorkers: number;
  p50TaskDurationMs: number;
  p95TaskDurationMs: number;
}
