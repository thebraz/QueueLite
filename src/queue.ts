import { QueueLiteError, storageError } from './errors.js';
import { Storage } from './storage.js';
import { Worker } from './worker.js';
import { integer, pagination, queueOptions, serializePayload, validateName, validateObject } from './validation.js';
import type { AddOptions, Job, JsonValue, ListOptions, PageOptions, QueueOptions, ShutdownOptions, TypedJob, WorkerOptions } from './types.js';

export class Queue<T extends object = Record<string, JsonValue>> {
  private readonly storage: Storage;
  private readonly workers = new Set<Worker<T>>();
  private readonly readOnly: boolean;
  private closed = false;
  private closing: Promise<void> | undefined;
  constructor(options: QueueOptions) {
    queueOptions(options);
    this.readOnly = options.readOnly ?? false;
    this.storage = new Storage(options.database, options);
  }
  private assertOpen = (): void => {
    if (this.closed) throw new QueueLiteError('Queue is closing or closed.');
  };
  private operation<R>(label: string, action: () => R): R {
    this.assertOpen();
    try { return action(); }
    catch (cause) { throw storageError(cause, label); }
  }
  add<K extends keyof T & string>(name: K, data: T[K], options: AddOptions = {}): Job<T[K]> & { name: K } {
    this.assertOpen();
    validateName(name);
    const payload = serializePayload(data);
    return this.operation('Could not enqueue job', () => this.storage.add(name, payload, options)) as Job<T[K]> & { name: K };
  }
  getJob(id: string): TypedJob<T> | undefined {
    this.assertOpen();
    this.validateId(id);
    return this.operation('Could not inspect job', () => this.storage.get(id)) as TypedJob<T> | undefined;
  }
  getJobSummary(id: string) {
    this.assertOpen(); this.validateId(id);
    return this.operation('Could not inspect job metadata', () => this.storage.summary(id));
  }
  private validateId(id: string): void {
    if (typeof id !== 'string' || !id.trim() || id.includes('\0')) throw new QueueLiteError('Job ID must be a non-empty string without null bytes.');
  }
  listJobs(options: ListOptions = {}) {
    this.assertOpen();
    const config = pagination(options);
    if (options.status !== undefined && !['pending', 'active', 'completed', 'failed', 'cancelled', 'delayed'].includes(options.status)) {
      throw new QueueLiteError('status must be pending, active, completed, failed, cancelled or delayed.');
    }
    return this.operation('Could not list jobs', () => this.storage.list({ ...config, ...(options.status === undefined ? {} : { status: options.status }) }));
  }
  getStats() { return this.operation('Could not read queue statistics', () => this.storage.stats()); }
  getAttempts(id: string, options: PageOptions = {}) {
    this.assertOpen(); this.validateId(id);
    const { limit, after } = pagination(options);
    return this.operation('Could not inspect job attempts', () => this.storage.attempts(id, limit, after));
  }
  getFailureHistory(id: string, options: PageOptions = {}) {
    this.assertOpen(); this.validateId(id);
    const { limit, after } = pagination(options);
    return this.operation('Could not inspect job failures', () => this.storage.attempts(id, limit, after, true));
  }
  getActiveClaims(options: PageOptions = {}) { return this.listJobs({ ...pagination(options), status: 'active' }); }
  getFailedJobs(limit = 100): TypedJob<T>[] {
    this.assertOpen();
    integer(limit, 'limit', 1, 1000);
    return this.operation('Could not list failed jobs', () => this.storage.failed(limit)) as TypedJob<T>[];
  }
  retryJob(id: string): TypedJob<T> {
    this.assertOpen(); this.validateId(id);
    return this.operation('Could not retry job', () => this.storage.retry(id)) as TypedJob<T>;
  }
  cancelJob(id: string): TypedJob<T> {
    this.assertOpen(); this.validateId(id);
    return this.operation('Could not cancel job', () => this.storage.cancel(id)) as TypedJob<T>;
  }
  createWorker(options: WorkerOptions = {}): Worker<T> {
    this.assertOpen();
    if (this.readOnly) throw new QueueLiteError('Cannot create a worker on read-only storage; open a writable queue.');
    const worker = new Worker<T>(this.storage, this.assertOpen, options);
    this.workers.add(worker);
    return worker;
  }
  close(options: ShutdownOptions = {}): Promise<void> {
    if (this.closing) return this.closing;
    validateObject(options, 'Shutdown options');
    if (options.timeoutMs !== undefined) integer(options.timeoutMs, 'timeoutMs', 0, 2_147_483_647);
    this.closed = true;
    this.closing = (async () => {
      const results = await Promise.allSettled([...this.workers].map((worker) => worker.stop(options)));
      this.storage.close();
      this.workers.clear();
      const failure = results.find((result) => result.status === 'rejected');
      if (failure?.status === 'rejected') throw failure.reason;
    })();
    return this.closing;
  }
}
export function createQueue<T extends object = Record<string, JsonValue>>(options: QueueOptions): Queue<T> {
  return new Queue<T>(options);
}
