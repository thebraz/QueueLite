export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };
export type JobStatus = 'pending' | 'active' | 'completed' | 'failed' | 'cancelled';
export interface BackoffOptions { type: 'fixed' | 'exponential'; delay: number; jitter?: number }
export interface JobError {
  attempt: number;
  at: number;
  kind: 'handler' | 'missing-handler' | 'lease-expired';
  name: string;
  message: string;
  code?: string;
}
export interface Job<T = JsonValue> {
  id: string;
  name: string;
  data: T;
  status: JobStatus;
  createdAt: number;
  updatedAt: number;
  attempts: number;
  runAt: number;
  startedAt: number | null;
  finishedAt: number | null;
  error: string | null;
  maxAttempts: number;
  backoff: BackoffOptions;
  priority: number;
  idempotencyKey: string | null;
  errorHistory: JobError[];
  leaseExpiresAt: number | null;
}
export type TypedJob<T extends object> = { [K in keyof T & string]: Job<T[K]> & { name: K } }[keyof T & string];
export interface QueueOptions {
  database: string;
  readOnly?: boolean;
  fileMustExist?: boolean;
  onEvent?: (event: LifecycleEvent) => void;
  logger?: { info(event: LifecycleEvent): void };
}
export type LifecycleEventType = 'enqueued' | 'started' | 'completed' | 'failed' | 'retried' | 'recovered' | 'cancelled';
export interface LifecycleEvent {
  readonly type: LifecycleEventType;
  readonly jobId: string;
  readonly at: number;
  readonly attempt: number;
  readonly status: JobStatus;
  readonly durationMs?: number;
}
export interface PageOptions { limit?: number; after?: number }
export interface ListOptions extends PageOptions { status?: JobStatus | 'delayed' }
export interface Page<T> { items: T[]; nextCursor: number | null }
/** Payload-free job metadata; pending includes delayed jobs unless filtered explicitly. */
export type JobSummary = Omit<Job, 'data' | 'error' | 'errorHistory' | 'idempotencyKey'>;
export interface JobAttempt {
  id: number;
  jobId: string;
  attempt: number;
  startedAt: number | null;
  finishedAt: number | null;
  outcome: 'active' | 'completed' | 'failed' | 'recovered';
  retry: boolean;
  error: JobError | null;
}
export interface QueueStats {
  total: number;
  pending: number;
  delayed: number;
  active: number;
  completed: number;
  failed: number;
  cancelled: number;
  retryAttempts: number;
  outcomes: { completed: number; failed: number; recovered: number };
  averageDurationMs: number | null;
}
export interface DiagnosticCheck { name: string; status: 'ok' | 'warning' | 'error'; message: string }
export interface DiagnosticReport { ok: boolean; schemaVersion: number | null; checks: DiagnosticCheck[] }
export interface AddOptions {
  runAt?: number;
  delay?: number;
  attempts?: number;
  backoff?: BackoffOptions;
  priority?: number;
  idempotencyKey?: string;
}
export interface WorkerStartOptions { concurrency?: number }
export interface ShutdownOptions { timeoutMs?: number }
export interface WorkerOptions extends WorkerStartOptions {
  pollIntervalMs?: number;
  leaseDurationMs?: number;
  heartbeatIntervalMs?: number;
  shutdownTimeoutMs?: number;
}
export interface JobContext { signal: AbortSignal }
export type JobHandler<T = JsonValue> = (job: Job<T>, context: JobContext) => void | Promise<void>;
