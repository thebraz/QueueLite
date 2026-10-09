import { afterEach, describe, expect, it, vi } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import Database from 'better-sqlite3';
import { createQueue, QueueLiteError, Worker } from '../src/index.js';
import type { AddOptions, Queue, WorkerOptions } from '../src/index.js';
import { Storage } from '../src/storage.js';
import { retryDelay, serializeError } from '../src/reliability.js';

const directories: string[] = [];
const queues: Queue[] = [];
const stores: Storage[] = [];
function path(): string {
  const directory = mkdtempSync(join(tmpdir(), 'queuelite-reliability-'));
  directories.push(directory); return join(directory, 'jobs.db');
}
function queue(database = path()): Queue {
  const q = createQueue({ database }); queues.push(q); return q;
}
function storage(database = path()): Storage {
  const s = new Storage(database); stores.push(s); return s;
}
function clock(now = 1000): void { vi.useFakeTimers(); vi.setSystemTime(now); }
async function until(check: () => boolean): Promise<void> {
  for (let i = 0; i < 500; i++) { if (check()) return; await sleep(5); }
  throw new Error('Condition timed out.');
}
function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => { resolve = done; });
  return { promise, resolve };
}
afterEach(async () => {
  vi.useRealTimers();
  await Promise.all(queues.splice(0).map((q) => q.close()));
  for (const s of stores.splice(0)) s.close();
  for (const directory of directories.splice(0)) rmSync(directory, { recursive: true, force: true });
});

describe('scheduling and retries', () => {
  it('persists delays across restart and claims at the exact UTC boundary', async () => {
    clock();
    const database = path(); const q = queue(database);
    const job = q.add('task', {}, { delay: 100 });
    await q.close();
    const reopened = storage(database);
    vi.setSystemTime(1099); expect(reopened.claim()).toBeUndefined();
    vi.setSystemTime(1100); expect(reopened.claim()?.job).toMatchObject({ id: job.id, startedAt: 1100 });
  });
  it.each(['fixed', 'exponential'] as const)('schedules %s retries, never early, and exhausts the total budget', (type) => {
    clock(); const s = storage();
    const job = s.add('task', '{}', { attempts: 3, backoff: { type, delay: 100 } });
    let now = 1000;
    for (let attempt = 1; attempt <= 3; attempt++) {
      const claim = s.claim(1000)!;
      expect(claim.job.attempts).toBe(attempt);
      expect(s.finish(job.id, claim.token, serializeError(new Error(`failure ${attempt}`), attempt, now))).toBe(true);
      const snapshot = s.get(job.id)!;
      expect(snapshot.errorHistory).toHaveLength(attempt);
      if (attempt < 3) {
        const delay = type === 'fixed' ? 100 : 100 * 2 ** (attempt - 1);
        expect(snapshot).toMatchObject({ status: 'pending', error: null, startedAt: null, finishedAt: null, runAt: now + delay });
        now += delay; vi.setSystemTime(now - 1); expect(s.claim()).toBeUndefined(); vi.setSystemTime(now);
      } else expect(snapshot).toMatchObject({ status: 'failed', attempts: 3, error: 'Error: failure 3', finishedAt: now });
    }
    vi.setSystemTime(now + 100000); expect(s.claim()).toBeUndefined();
  });
  it('bounds jitter, overflow and zero-delay exponential retries deterministically', () => {
    const b = { type: 'exponential' as const, delay: 100, jitter: 0.25 };
    expect(retryDelay(b, 3, () => 0)).toBe(400);
    expect(retryDelay(b, 3, () => 1)).toBe(300);
    expect(retryDelay(b, 3, () => 0.5)).toBe(350);
    expect(retryDelay({ type: 'fixed', delay: 100, jitter: 1 }, 9, () => 1)).toBe(0);
    expect(retryDelay({ type: 'exponential', delay: 100 }, 1000)).toBe(Number.MAX_SAFE_INTEGER);
    expect(retryDelay({ type: 'exponential', delay: 0 }, 1000)).toBe(0);
  });
  it('persists the jittered retry timestamp', () => {
    clock(); vi.spyOn(Math, 'random').mockReturnValue(0.5);
    try {
      const s = storage(); const job = s.add('task', '{}', { attempts: 2, backoff: { type: 'fixed', delay: 100, jitter: 0.4 } });
      const c = s.claim()!;
      s.finish(job.id, c.token, serializeError('retry', 1, 1000));
      expect(s.get(job.id)?.runAt).toBe(1080);
    } finally { vi.restoreAllMocks(); }
  });
  it('orders eligible jobs by descending priority then persistent insertion FIFO', () => {
    clock(); const s = storage();
    const low = s.add('task', '{}', { priority: -1 });
    const a = s.add('task', '{}', { priority: 2 });
    const b = s.add('task', '{}', { priority: 2, runAt: 0 });
    const delayed = s.add('task', '{}', { priority: 99, delay: 100 });
    expect([s.claim()?.job.id, s.claim()?.job.id, s.claim()?.job.id]).toEqual([a.id, b.id, low.id]);
    expect(s.claim()).toBeUndefined(); vi.setSystemTime(1100); expect(s.claim()?.job.id).toBe(delayed.id);
  });
  it('inspects terminal failures and manually resets only their attempt budget, retaining history', async () => {
    const q = queue(); const job = q.add('task', {}, { attempts: 2, idempotencyKey: 'key' });
    let fail = true;
    const worker = q.createWorker({ pollIntervalMs: 2 }).register('task', () => { if (fail) throw new Error('failed'); });
    const running = worker.start();
    await until(() => q.getJob(job.id)?.status === 'failed');
    expect(q.getFailedJobs().map((j) => j.id)).toEqual([job.id]);
    fail = false;
    expect(q.retryJob(job.id)).toMatchObject({ status: 'pending', attempts: 0, maxAttempts: 2, errorHistory: expect.any(Array) });
    expect(q.add('task', {}, { idempotencyKey: 'key' }).id).toBe(job.id);
    await until(() => q.getJob(job.id)?.status === 'completed');
    await worker.stop(); await running;
    expect(q.getJob(job.id)).toMatchObject({ attempts: 1, errorHistory: [expect.any(Object), expect.any(Object)] });
    expect(q.getFailedJobs()).toEqual([]);
    expect(() => q.retryJob(job.id)).toThrow('Only failed');
  });
  it('marks unavailable handlers terminal immediately without an automatic retry storm', async () => {
    const q = queue(); const job = q.add('missing', {}, { attempts: 1000 });
    const w = q.createWorker({ pollIntervalMs: 2 });
    await w.drain();
    expect(q.getJob(job.id)).toMatchObject({ status: 'failed', attempts: 1, errorHistory: [{ kind: 'missing-handler', attempt: 1, at: expect.any(Number), name: 'QueueLiteError', message: expect.stringContaining('No handler') }] });
    w.register('missing', () => {}); q.retryJob(job.id); await w.drain();
    expect(q.getJob(job.id)?.status).toBe('completed');
  });
  it('cancels only pending jobs, and never claims cancelled or completed jobs', () => {
    const database = path(); const q = queue(database); const s = storage(database);
    const job = q.add('task', {});
    expect(q.cancelJob(job.id).status).toBe('cancelled');
    expect(() => q.retryJob(job.id)).toThrow('Only failed');
    expect(() => q.cancelJob(job.id)).toThrow('Only pending');
    expect(() => q.retryJob('missing')).toThrow();
    expect(s.claim()).toBeUndefined();
  });
  it('cancellation and claiming are serialized across connections', () => {
    const database = path(); const q = queue(database); const s = storage(database);
    const cancelled = q.add('task', {}); q.cancelJob(cancelled.id);
    expect(s.claim()).toBeUndefined();
    const active = q.add('task', {}); const claim = s.claim()!;
    expect(() => q.cancelJob(active.id)).toThrow('Only pending');
    s.finish(active.id, claim.token, null);
    expect(() => q.cancelJob(active.id)).toThrow();
  });
  it('deduplicates simultaneous submissions, scopes keys by name and retains them in terminal states', async () => {
    const database = path(); const a = queue(database); const b = queue(database);
    const jobs = await Promise.all(Array.from({ length: 100 }, (_, i) => Promise.resolve().then(() => (i % 2 ? a : b).add('task', { i }, { idempotencyKey: 'same' }))));
    expect(new Set(jobs.map((j) => j.id)).size).toBe(1);
    expect(a.add('other', {}, { idempotencyKey: 'same' }).id).not.toBe(jobs[0]!.id);
    const s = storage(database); const claim = s.claim()!; s.finish(claim.job.id, claim.token, null);
    expect(b.add('task', { changed: true }, { idempotencyKey: 'same' })).toEqual(a.getJob(jobs[0]!.id));
  });
});

describe('leases and ownership', () => {
  it('recovers expiration exactly once and fences stale success, failure and heartbeat writes', () => {
    clock(); const database = path(); const a = storage(database); const b = storage(database);
    const job = a.add('task', '{}', { attempts: 3 }); const old = a.claim(100)!;
    vi.setSystemTime(1099); expect(b.claim()).toBeUndefined();
    vi.setSystemTime(1100);
    expect(a.finish(job.id, old.token, null)).toBe(false);
    expect(a.renew(job.id, old.token, 100)).toBe(false);
    const current = b.claim(100)!;
    expect(current.job).toMatchObject({ id: job.id, attempts: 2, errorHistory: [expect.objectContaining({ kind: 'lease-expired' })] });
    expect(current.token).not.toBe(old.token);
    const snapshot = b.get(job.id);
    expect(a.finish(job.id, old.token, serializeError('stale', 1, 1100))).toBe(false);
    expect(a.renew(job.id, old.token, 100)).toBe(false);
    expect(b.get(job.id)).toEqual(snapshot);
    expect(b.finish(job.id, current.token, null)).toBe(true);
    expect(b.finish(job.id, current.token, null)).toBe(false);
  });
  it('renews leases and disallows resurrection of an expired claim', () => {
    clock(); const s = storage(); const job = s.add('task', '{}', { attempts: 2 }); const claim = s.claim(100)!;
    vi.setSystemTime(1050); expect(s.renew(job.id, claim.token, 100)).toBe(true);
    vi.setSystemTime(1100); expect(s.claim()).toBeUndefined();
    vi.setSystemTime(1150); expect(s.renew(job.id, claim.token, 100)).toBe(false);
    expect(s.claim()?.job.attempts).toBe(2);
  });
  it('exhausts crash attempts instead of recovering forever', () => {
    clock(); const s = storage(); const job = s.add('task', '{}', { attempts: 2 });
    s.claim(100); vi.setSystemTime(1100); s.claim(100); vi.setSystemTime(1200);
    expect(s.claim()).toBeUndefined();
    expect(s.get(job.id)).toMatchObject({ status: 'failed', attempts: 2, leaseExpiresAt: null, errorHistory: [expect.any(Object), expect.any(Object)] });
  });
  it('applies configured backoff to crash recovery', () => {
    clock(); const s = storage(); const job = s.add('task', '{}', { attempts: 2, backoff: { type: 'fixed', delay: 50 } });
    s.claim(100); vi.setSystemTime(1100); expect(s.claim()).toBeUndefined();
    expect(s.get(job.id)).toMatchObject({ status: 'pending', runAt: 1150 });
    vi.setSystemTime(1150); expect(s.claim()?.job.id).toBe(job.id);
  });
  it('drains recovery batches without leaving expired exhausted claims behind', async () => {
    const database = path(); const q = queue(database); const s = storage(database);
    const jobs = Array.from({ length: 205 }, () => s.add('task', '{}'));
    for (const job of jobs) expect(s.claim(60000)?.job.id).toBe(job.id);
    const db = new Database(database);
    try { db.prepare("UPDATE jobs SET lease_expires_at = 0 WHERE status = 'active'").run(); }
    finally { db.close(); }
    await q.createWorker({ pollIntervalMs: 1 }).drain();
    expect(jobs.every((job) => q.getJob(job.id)?.status === 'failed')).toBe(true);
  }, 20000); // Hundreds of durable writes can exceed the default timeout on CI disks.
  it('migrates version 1 atomically, preserving payloads, terminal errors and recovering active jobs', () => {
    const database = path(); const db = new Database(database);
    db.exec(`CREATE TABLE jobs (id TEXT PRIMARY KEY, name TEXT, payload TEXT, status TEXT, created_at INTEGER, updated_at INTEGER,
      attempts INTEGER, run_at INTEGER, started_at INTEGER, finished_at INTEGER, error TEXT);
      CREATE INDEX jobs_pending ON jobs(run_at, created_at, id) WHERE status = 'pending';
      INSERT INTO jobs VALUES ('pending','task','{"saved":true}','pending',1,1,0,1,NULL,NULL,NULL),
      ('active','task','{}','active',1,2,1,1,2,NULL,NULL), ('failed','task','{}','failed',1,3,1,1,2,3,'legacy error'),
      ('completed','task','{}','completed',1,3,1,1,2,3,NULL); PRAGMA user_version = 1;`);
    db.close();
    const s = storage(database);
    expect(s.get('pending')?.data).toEqual({ saved: true });
    expect(s.get('failed')).toMatchObject({ status: 'failed', error: 'legacy error' });
    expect(s.get('completed')?.status).toBe('completed');
    expect(s.claim()?.job.id).toBe('pending');
    expect(s.claim()?.job).toMatchObject({ id: 'active', attempts: 2 });
    const check = new Database(database);
    try { expect(check.pragma('integrity_check')).toEqual([{ integrity_check: 'ok' }]); }
    finally { check.close(); }
  });
  it('rolls a failed migration back without dropping original data, indexes or version', () => {
    const database = path(); const db = new Database(database);
    db.exec(`CREATE TABLE jobs (id TEXT PRIMARY KEY, name TEXT, payload TEXT, status TEXT, created_at INTEGER, updated_at INTEGER,
      attempts INTEGER, run_at INTEGER, started_at INTEGER, finished_at INTEGER, error TEXT);
      CREATE INDEX jobs_pending ON jobs(run_at, created_at, id) WHERE status = 'pending';
      INSERT INTO jobs VALUES ('keep','task','invalid-json','pending',1,1,0,1,NULL,NULL,NULL); PRAGMA user_version = 1;`);
    db.close(); expect(() => new Storage(database)).toThrow('initialize');
    const check = new Database(database);
    try {
      expect(check.pragma('user_version', { simple: true })).toBe(1);
      expect(check.prepare('SELECT id, payload FROM jobs').get()).toEqual({ id: 'keep', payload: 'invalid-json' });
      expect(check.prepare("SELECT name FROM sqlite_master WHERE name = 'jobs_pending'").get()).toBeDefined();
      expect(check.prepare("SELECT name FROM sqlite_master WHERE name = 'jobs_v1'").get()).toBeUndefined();
    } finally { check.close(); }
  });
});

describe('worker reliability and lifecycle', () => {
  it('runs parallel handlers and never exceeds start-time concurrency', async () => {
    const q = queue(); const jobs = Array.from({ length: 12 }, () => q.add('task', {}));
    const gate = deferred(); let active = 0; let maximum = 0;
    const w = q.createWorker({ pollIntervalMs: 2 }).register('task', async () => {
      maximum = Math.max(maximum, ++active); await gate.promise; active--;
    });
    const running = w.start({ concurrency: 3 });
    try {
      await until(() => active === 3); await sleep(15);
      expect(q.getJob(jobs[3]!.id)?.status).toBe('pending'); expect(maximum).toBe(3);
    } finally { gate.resolve(); }
    await until(() => jobs.every((j) => q.getJob(j.id)?.status === 'completed'));
    await w.stop(); await running; expect(maximum).toBe(3);
  });
  it('enforces per-worker limits while multiple instances share one database', async () => {
    const database = path(); const a = queue(database); const b = queue(database);
    const jobs = Array.from({ length: 25 }, () => a.add('task', {}));
    const seen: string[] = []; const active = [0, 0]; const maxima = [0, 0];
    const workers = [a, b].map((q, i) => q.createWorker({ pollIntervalMs: 2, concurrency: 2 }).register('task', async (job) => {
      active[i] = active[i]! + 1; maxima[i] = Math.max(maxima[i]!, active[i]!); seen.push(job.id); await sleep(10); active[i]!--;
    }));
    const running = workers.map((w) => w.start());
    await until(() => jobs.every((j) => a.getJob(j.id)?.status === 'completed'));
    await Promise.all(workers.map((w) => w.stop())); await Promise.all(running);
    expect(maxima).toEqual([2, 2]); expect(new Set(seen).size).toBe(25); expect(seen).toHaveLength(25);
  });
  it('renews heartbeats under a controllable clock and prevents a second worker from reclaiming', async () => {
    clock(); const database = path(); const q = queue(database); const s = storage(database);
    const job = q.add('task', {}, { attempts: 2 }); const gate = deferred();
    const w = q.createWorker({ pollIntervalMs: 10, leaseDurationMs: 90, heartbeatIntervalMs: 20 }).register('task', () => gate.promise);
    const running = w.start();
    try {
      await vi.advanceTimersByTimeAsync(250);
      expect(q.getJob(job.id)).toMatchObject({ status: 'active', attempts: 1 });
      expect(q.getJob(job.id)!.leaseExpiresAt).toBeGreaterThan(Date.now()); expect(s.claim()).toBeUndefined();
    } finally {
      gate.resolve(); await vi.advanceTimersByTimeAsync(1); const stopping = w.stop(); await vi.advanceTimersByTimeAsync(10); await stopping; await running;
    }
    expect(q.getJob(job.id)?.status).toBe('completed');
  });
  it('pauses claims, allows active work to finish, resumes, and drains only eligible work', async () => {
    const q = queue(); const first = q.add('task', {}); const second = q.add('task', {});
    const future = q.add('task', {}, { delay: 60000 }); const gate = deferred();
    const w = q.createWorker({ pollIntervalMs: 2 }).register('task', () => gate.promise);
    const running = w.start(); await until(() => q.getJob(first.id)?.status === 'active');
    w.pause(); gate.resolve(); await until(() => q.getJob(first.id)?.status === 'completed');
    await sleep(15); expect(q.getJob(second.id)?.status).toBe('pending');
    w.resume(); await w.drain(); await running;
    expect(q.getJob(second.id)?.status).toBe('completed'); expect(q.getJob(future.id)?.status).toBe('pending');
  });
  it('waits for every active handler during graceful stop, claims no additional jobs', async () => {
    const q = queue(); const jobs = Array.from({ length: 4 }, () => q.add('task', {}));
    const gate = deferred(); let entered = 0; let stopped = false;
    const w = q.createWorker({ concurrency: 3, pollIntervalMs: 2 }).register('task', async () => { entered++; await gate.promise; });
    const running = w.start(); await until(() => entered === 3);
    const stopping = w.stop().then(() => { stopped = true; }); await sleep(10); expect(stopped).toBe(false);
    gate.resolve(); await stopping; await running;
    expect(jobs.map((j) => q.getJob(j.id)?.status)).toEqual(['completed', 'completed', 'completed', 'pending']);
  });
  it('honors shutdown deadlines, aborts cooperatively, fences late handlers and closes SQLite safely', async () => {
    const database = path(); const q = queue(database); const job = q.add('task', {}, { attempts: 2 });
    const gate = deferred(); let signal!: AbortSignal;
    const w = q.createWorker({ leaseDurationMs: 100, heartbeatIntervalMs: 20, pollIntervalMs: 2 }).register('task', async (_, context) => {
      signal = context.signal; await gate.promise;
    });
    const running = w.start(); await until(() => signal !== undefined);
    await expect(w.stop({ timeoutMs: 10 })).rejects.toThrow('deadline'); await running;
    expect(signal.aborted).toBe(true); expect(q.getJob(job.id)?.status).toBe('active');
    expect(() => w.start()).toThrow('unfinished'); await q.close();
    const s = storage(database); await sleep(110); const claim = s.claim()!;
    expect(claim.job.attempts).toBe(2); s.finish(job.id, claim.token, null);
    gate.resolve(); await sleep(10); expect(s.get(job.id)).toMatchObject({ status: 'completed', attempts: 2 });
  });
  it('applies a configured queue-close deadline and still releases the connection', async () => {
    const database = path(); const q = queue(database); const gate = deferred(); const job = q.add('task', {}, { attempts: 2 });
    const w = q.createWorker({ shutdownTimeoutMs: 5, pollIntervalMs: 2 }).register('task', () => gate.promise);
    const running = w.start(); await until(() => q.getJob(job.id)?.status === 'active');
    await expect(q.close()).rejects.toThrow('deadline'); await running;
    queues.splice(queues.indexOf(q), 1);
    expect(() => q.getJob(job.id)).toThrow('closed');
    expect(storage(database).get(job.id)?.status).toBe('active'); gate.resolve(); await sleep(5);
  });
  it('yields during synchronous jobs so lifecycle timers run', async () => {
    const q = queue(); let count = 0;
    for (let i = 0; i < 100; i++) q.add('task', {});
    const w = q.createWorker({ pollIntervalMs: 1, concurrency: 5 }).register('task', () => { count++; });
    const running = w.start(); await sleep(0); await w.stop(); await running;
    expect(count).toBeGreaterThan(0); expect(count).toBeLessThan(100);
  });
  it('surfaces fatal heartbeat failures through the lifetime promise and fences late handlers', async () => {
    const s = storage(); const job = s.add('task', '{}', { attempts: 2 }); const gate = deferred();
    let signal!: AbortSignal;
    const w = new Worker(s, () => {}, { leaseDurationMs: 90, heartbeatIntervalMs: 10, pollIntervalMs: 2 });
    w.register('task', (_, context) => { signal = context.signal; return gate.promise; });
    const spy = vi.spyOn(s, 'renew').mockImplementation(() => { throw new Error('Fatal engine failure'); });
    const running = w.start(); const rejection = expect(running).rejects.toMatchObject({
      name: 'QueueLiteError', message: expect.stringContaining('Worker storage operation failed'),
      cause: expect.objectContaining({ message: 'Fatal engine failure' }), retryable: false,
    });
    try {
      await rejection; expect(signal.aborted).toBe(true);
      expect(s.get(job.id)?.status).toBe('active');
    } finally { spy.mockRestore(); gate.resolve(); await sleep(5); await w.stop(); }
    expect(s.get(job.id)?.status).toBe('active');
  });
  it('surfaces final-write errors as engine failures without misreporting successful handlers', async () => {
    const s = storage(); const job = s.add('task', '{}');
    const w = new Worker(s, () => {}, { pollIntervalMs: 2 }); let executed = 0;
    w.register('task', () => { executed++; });
    const spy = vi.spyOn(s, 'finish').mockImplementation(() => { throw new Error('Disk unavailable'); });
    try { await expect(w.start()).rejects.toMatchObject({ name: 'QueueLiteError',
      cause: expect.objectContaining({ message: 'Disk unavailable' }), retryable: false }); }
    finally { spy.mockRestore(); await w.stop(); }
    expect(executed).toBe(1); expect(s.get(job.id)).toMatchObject({ status: 'active', error: null, errorHistory: [] });
  });
  it('cancels long contested-write sleeps at shutdown instead of retaining retry timers', async () => {
    const s = storage(); s.add('task', '{}', { attempts: 2 });
    const w = new Worker(s, () => {}, { pollIntervalMs: 60000, heartbeatIntervalMs: 10000 });
    w.register('task', () => {});
    const spy = vi.spyOn(s, 'finish').mockImplementation(() => { throw Object.assign(new Error('locked'), { code: 'SQLITE_BUSY' }); });
    const running = w.start();
    try {
      await until(() => spy.mock.calls.length > 0);
      await expect(w.stop({ timeoutMs: 0 })).rejects.toThrow('deadline'); await running;
    } finally { spy.mockRestore(); }
    await sleep(5);
    const restarted = w.start(); await w.stop(); await restarted;
  });
});

describe('trust boundaries', () => {
  it.each([
    { attempts: 0 }, { attempts: 1001 }, { attempts: 1.5 }, { attempts: null },
    { delay: -1 }, { delay: Infinity }, { delay: 1, runAt: 2 }, { delay: Number.MAX_SAFE_INTEGER },
    { priority: Infinity }, { priority: 2 ** 31 }, { priority: 1.5 }, { idempotencyKey: '' }, { idempotencyKey: null },
    { idempotencyKey: 'x'.repeat(256) }, { idempotencyKey: 'bad\0key' },
    { backoff: null }, { backoff: {} }, { backoff: { type: 'other', delay: 1 } },
    { backoff: { type: 'fixed', delay: -1 } }, { backoff: { type: 'fixed', delay: 1, jitter: 1.1 } },
    { backoff: { type: 'fixed', delay: 1, jitter: NaN } },
  ])('rejects invalid enqueue options %#', (options) => {
    expect(() => queue().add('task', {}, options as AddOptions)).toThrow(QueueLiteError);
  });
  it.each([{ concurrency: 0 }, { concurrency: 1001 }, { leaseDurationMs: 2 }, { heartbeatIntervalMs: 30000 },
    { heartbeatIntervalMs: 0 }, { shutdownTimeoutMs: -1 }, { concurrency: null }])('rejects invalid worker options %#', (options) => {
    expect(() => queue().createWorker(options as WorkerOptions)).toThrow(QueueLiteError);
  });
  it('validates new lifecycle and inspection options without closing a usable queue', async () => {
    const q = queue(); const w = q.createWorker();
    expect(() => w.start({ concurrency: 0 })).toThrow();
    await expect(w.stop({ timeoutMs: -1 })).rejects.toThrow();
    await expect(w.drain(null!)).rejects.toThrow();
    expect(() => q.close({ timeoutMs: -1 })).toThrow();
    expect(() => q.getFailedJobs(0)).toThrow(); expect(() => q.cancelJob('')).toThrow();
    expect(q.add('task', {})).toBeDefined();
  });
  it('serializes errors defensively and redacts common secrets without traversing arbitrary metadata', () => {
    const error = Object.assign(new Error('password=canary Bearer canary2 https://user:canary3@example.test/?token=canary4 "token":"canary7" Authorization: Basic canary8'), {
      code: 'E_DELIVERY', secret: 'canary5', cause: new Error('canary6'),
    });
    const saved = serializeError(error, 2, 100);
    expect(saved.code).toBe('E_DELIVERY'); expect(saved.message).toContain('[REDACTED]');
    expect(JSON.stringify(saved)).not.toMatch(/canary/); expect(saved).not.toHaveProperty('stack');
    const getter = new Error(); Object.defineProperty(getter, 'message', { get: () => { throw new Error('getter'); } });
    expect(() => serializeError(getter, 1, 1)).not.toThrow();
    expect(serializeError(new Proxy({}, { getPrototypeOf: () => { throw new Error(); } }), 1, 1).message).toContain('unprintable');
    expect(serializeError('x'.repeat(10000), 1, 1).message).toHaveLength(2048);
    expect(serializeError(new TypeError('invalid'), 1, 1).name).toBe('TypeError');
  });
});
