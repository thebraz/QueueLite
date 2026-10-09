import { setTimeout as sleep } from 'node:timers/promises';
import { QueueLiteError, storageError } from './errors.js';
import { integer, validateName, validateObject } from './validation.js';
import { isBusy, serializeError } from './reliability.js';
import type { Claim, Storage } from './storage.js';
import type { Job, JobHandler, JsonValue, ShutdownOptions, WorkerOptions, WorkerStartOptions } from './types.js';

interface Execution {
  id: string;
  token: string;
  controller: AbortController;
  owned: boolean;
  heartbeat?: ReturnType<typeof setInterval>;
}

export class Worker<T extends object = Record<string, JsonValue>> {
  private readonly handlers = new Map<string, JobHandler>();
  private readonly active = new Set<Execution>();
  private running = false;
  private paused = false;
  private draining = false;
  private abandoned = false;
  private failure: { cause: unknown } | undefined;
  private task: Promise<void> | undefined;
  private wake: AbortController | undefined;
  private readonly pollIntervalMs: number;
  private readonly leaseDurationMs: number;
  private readonly heartbeatIntervalMs: number;
  private readonly shutdownTimeoutMs: number | undefined;
  private readonly defaultConcurrency: number;
  private concurrency = 1;

  /** @internal Create workers through queue.createWorker(). */
  constructor(private readonly storage: Storage, private readonly assertOpen: () => void, options: WorkerOptions = {}) {
    validateObject(options, 'Worker options');
    this.pollIntervalMs = integer(options.pollIntervalMs === undefined ? 100 : options.pollIntervalMs, 'pollIntervalMs', 1, 2_147_483_647);
    this.leaseDurationMs = integer(options.leaseDurationMs === undefined ? 30000 : options.leaseDurationMs, 'leaseDurationMs', 3, 2_147_483_647);
    this.heartbeatIntervalMs = integer(options.heartbeatIntervalMs === undefined ? Math.floor(this.leaseDurationMs / 3) : options.heartbeatIntervalMs,
      'heartbeatIntervalMs', 1, this.leaseDurationMs - 1);
    this.defaultConcurrency = integer(options.concurrency === undefined ? 1 : options.concurrency, 'concurrency', 1, 1000);
    if (options.shutdownTimeoutMs !== undefined) integer(options.shutdownTimeoutMs, 'shutdownTimeoutMs', 0, 2_147_483_647);
    this.shutdownTimeoutMs = options.shutdownTimeoutMs;
  }
  register<K extends keyof T & string>(name: K, handler: JobHandler<T[K]>): this {
    this.assertOpen();
    validateName(name);
    if (typeof handler !== 'function') throw new QueueLiteError('Handler must be a function.');
    if (this.handlers.has(name)) throw new QueueLiteError(`Handler already registered for "${name}".`);
    this.handlers.set(name, (job, context) => handler(job as Job<T[K]>, context));
    return this;
  }
  /** Resolves when stopped; rejects on storage/engine failures. Always observe this promise. */
  start(options: WorkerStartOptions = {}): Promise<void> {
    this.assertOpen();
    if (this.task || this.active.size) throw new QueueLiteError('Worker is already running, stopping, or has unfinished handlers.');
    validateObject(options, 'Start options');
    this.concurrency = integer(options.concurrency === undefined ? this.defaultConcurrency : options.concurrency, 'concurrency', 1, 1000);
    this.running = true; this.paused = false; this.draining = false; this.abandoned = false; this.failure = undefined;
    this.task = Promise.resolve().then(() => this.loop()).finally(() => { this.running = false; this.task = undefined; });
    return this.task;
  }
  pause(): void { this.assertOpen(); this.paused = true; this.wake?.abort(); }
  resume(): void { this.assertOpen(); this.paused = false; this.wake?.abort(); }
  async stop(options: ShutdownOptions = {}): Promise<void> {
    const timeout = this.timeout(options);
    this.running = false;
    this.wake?.abort();
    await this.waitForStop(timeout);
  }
  async drain(options: ShutdownOptions = {}): Promise<void> {
    this.assertOpen();
    const timeout = this.timeout(options);
    if (!this.task) this.start();
    this.draining = true; this.paused = false;
    this.wake?.abort();
    await this.waitForStop(timeout);
  }
  private timeout(options: ShutdownOptions): number | undefined {
    validateObject(options, 'Shutdown options');
    const timeout = options.timeoutMs === undefined ? this.shutdownTimeoutMs : options.timeoutMs;
    if (timeout !== undefined) integer(timeout, 'timeoutMs', 0, 2_147_483_647);
    return timeout;
  }
  private async waitForStop(timeout: number | undefined): Promise<void> {
    const task = this.task;
    if (!task) return;
    if (timeout === undefined) return task;
    const timer = new AbortController();
    try {
      const done = await Promise.race([task.then(() => true), sleep(timeout, false, { signal: timer.signal })]);
      if (!done) {
        this.running = false; this.abandon(); this.wake?.abort();
        await task;
        throw new QueueLiteError('Shutdown deadline exceeded; unfinished claims will recover after lease expiration.');
      }
    } finally { timer.abort(); }
  }
  private abandon(): void {
    this.abandoned = true;
    for (const state of this.active) this.lose(state);
  }
  private lose(state: Execution): void {
    state.owned = false;
    clearInterval(state.heartbeat);
    state.controller.abort();
  }
  private failEngine(cause: unknown): void {
    this.failure ??= { cause: storageError(cause, 'Worker storage operation failed') };
    this.running = false; this.abandon(); this.wake?.abort();
  }
  private launch(claim: Claim): void {
    const state: Execution = { id: claim.job.id, token: claim.token, controller: new AbortController(), owned: true };
    this.active.add(state);
    state.heartbeat = setInterval(() => {
      if (!state.owned) return;
      try {
        if (!this.storage.renew(state.id, state.token, this.leaseDurationMs)) this.lose(state);
      } catch (cause) { if (!isBusy(cause)) this.failEngine(cause); }
    }, this.heartbeatIntervalMs);
    // Track the execution before entering user code, including synchronous stop requests.
    void Promise.resolve().then(() => this.execute(claim, state)).catch((cause: unknown) => this.failEngine(cause)).finally(() => {
      clearInterval(state.heartbeat);
      this.active.delete(state);
      this.wake?.abort();
    });
  }
  private async execute(claim: Claim, state: Execution): Promise<void> {
    if (!state.owned) return;
    const attempt = claim.job.attempts;
    const handler = this.handlers.get(claim.job.name);
    let error = null;
    try {
      if (!handler) throw new QueueLiteError(`No handler registered for "${claim.job.name}".`);
      await handler(claim.job, { signal: state.controller.signal });
    } catch (cause) { error = serializeError(cause, attempt, Date.now(), handler ? 'handler' : 'missing-handler'); }
    while (state.owned) {
      try {
        if (!this.storage.finish(state.id, state.token, error, !handler)) this.lose(state);
        return;
      } catch (cause) {
        if (!isBusy(cause)) throw cause;
        try {
          await sleep(Math.min(this.pollIntervalMs, this.heartbeatIntervalMs), undefined, { signal: state.controller.signal });
        } catch (cause) { if (!state.controller.signal.aborted) throw cause; }
      }
    }
  }
  private async wait(): Promise<void> {
    const wake = new AbortController();
    this.wake = wake;
    try { await sleep(this.pollIntervalMs, undefined, { signal: wake.signal }); }
    catch (cause) { if (!wake.signal.aborted) throw cause; }
    finally { this.wake = undefined; }
  }
  private async loop(): Promise<void> {
    try {
      while (!this.failure) {
        if (!this.running && (this.active.size === 0 || this.abandoned)) break;
        let claimed = false;
        let busy = false;
        if (this.running && !this.paused) {
          while (this.running && this.active.size < this.concurrency) {
            let claim: Claim | undefined;
            try { claim = this.storage.claim(this.leaseDurationMs); }
            catch (cause) { if (!isBusy(cause)) throw cause; busy = true; break; }
            if (!claim) break;
            claimed = true; this.launch(claim);
          }
          if (this.draining && !busy && !claimed && this.active.size === 0 && !this.storage.hasEligible()) this.running = false;
        }
        if (claimed) { await sleep(0); continue; }
        if (!this.running && (this.active.size === 0 || this.abandoned)) break;
        await this.wait();
      }
      if (this.failure) throw this.failure.cause;
    } catch (cause) { this.abandon(); throw storageError(cause, 'Worker processing failed'); }
  }
}
