import { afterEach, describe, expect, it } from 'vitest';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { setTimeout } from 'node:timers/promises';
import Database from 'better-sqlite3';
import { createQueue, QueueLiteError, Worker } from '../src/index.js';
import type { Job, JsonValue, Queue } from '../src/index.js';
import { Storage } from '../src/storage.js';

const directories: string[] = [];
const queues: Queue[] = [];
function database(): string {
  const directory = mkdtempSync(join(tmpdir(), 'queuelite-'));
  directories.push(directory);
  return join(directory, 'queue.db');
}
function queue(path = database()): Queue {
  const instance = createQueue({ database: path });
  queues.push(instance);
  return instance;
}
async function terminal(instance: Queue, id: string): Promise<Job> {
  for (let i = 0; i < 400; i++) {
    const job = instance.getJob(id)!;
    if (job.status === 'completed' || job.status === 'failed') return job;
    await setTimeout(5);
  }
  throw new Error('Job did not reach a terminal state.');
}
afterEach(async () => {
  await Promise.all(queues.splice(0).map((instance) => instance.close()));
  for (const directory of directories.splice(0)) rmSync(directory, { recursive: true, force: true });
});

describe('persistent queue', () => {
  it('initializes a versioned and indexed WAL database', () => {
    const path = database();
    queue(path);
    const db = new Database(path);
    try {
      expect(db.pragma('journal_mode', { simple: true })).toBe('wal');
      expect(db.pragma('user_version', { simple: true })).toBe(3);
      expect(db.prepare("SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'jobs_pending'").get()).toBeDefined();
    } finally { db.close(); }
  });
  it('creates stable jobs, snapshots payloads and retrieves missing IDs', () => {
    const instance = queue();
    const data = { nested: [1, true, null, 'á'], name: "Robert'); DROP TABLE jobs;--" };
    const job = instance.add('task', data);
    data.nested.push('changed');
    expect(job).toMatchObject({ status: 'pending', attempts: 0, startedAt: null, finishedAt: null, error: null });
    expect(instance.getJob(job.id)?.data).toEqual({ ...data, nested: [1, true, null, 'á'] });
    expect(instance.getJob('missing')).toBeUndefined();
    expect(instance.add('task', null).id).not.toBe(job.id);
  });
  it('persists through instance restarts', async () => {
    const path = database();
    const first = queue(path);
    const job = first.add('task', { nested: ['saved'] }, { runAt: Date.now() + 10000 });
    await first.close();
    expect(queue(path).getJob(job.id)).toEqual(job);
  });
  it('persists through real process restarts', async () => {
    const path = database();
    const fixture = fileURLToPath(new URL('./fixtures/process.mjs', import.meta.url));
    const execute = promisify(execFile);
    const written = await execute(process.execPath, [fixture, 'write', path]);
    const read = await execute(process.execPath, [fixture, 'read', path, written.stdout.trim()]);
    expect(JSON.parse(read.stdout)).toMatchObject({ status: 'pending', data: { text: 'persisted', nested: [1, null, true] } });
  });
  it('rejects newer schema versions without replacing data', () => {
    const path = database();
    const db = new Database(path);
    db.exec('CREATE TABLE sentinel (value TEXT); INSERT INTO sentinel VALUES (\'keep\'); PRAGMA user_version = 99;');
    db.close();
    expect(() => queue(path)).toThrow('initialize');
    const check = new Database(path);
    try { expect(check.prepare('SELECT value FROM sentinel').get()).toEqual({ value: 'keep' }); }
    finally { check.close(); }
  });
});

describe('validation', () => {
  it.each(['', ' ', ' leading', 'trailing ', 'x'.repeat(256), '\0hidden'])('rejects invalid name %j', (name) => {
    expect(() => queue().add(name, {})).toThrow(QueueLiteError);
  });
  it.each([undefined, () => {}, 1n, NaN, Infinity, { value: undefined }, [undefined], new Date(), new Map(), Array(2), { [Symbol('key')]: 1 }])('rejects non-JSON payload case %#', (data) => {
    expect(() => queue().add('task', data as JsonValue)).toThrow(QueueLiteError);
  });
  it('rejects cycles and accessors but accepts repeated references', () => {
    const instance = queue();
    const cycle: { self?: unknown } = {}; cycle.self = cycle;
    expect(() => instance.add('task', cycle as JsonValue)).toThrow();
    expect(() => instance.add('task', { get value(): number { throw new Error('Must not execute'); } })).toThrow('accessors');
    const shared = { value: 1 };
    expect(instance.add('task', [shared, shared]).data).toEqual([shared, shared]);
  });
  it('rejects sparse arrays even when an extra property masks the missing index', () => {
    const data = Object.assign([1, , 3], { extra: 'lost' });
    expect(() => queue().add('task', data as unknown as JsonValue)).toThrow(QueueLiteError);
  });
  it('rejects inherited array serializers without executing them or storing a replacement payload', () => {
    const instance = queue();
    let calls = 0;
    const data = [1];
    Object.setPrototypeOf(data, { get toJSON() { calls++; return () => 'replacement'; } });
    expect(() => instance.add('task', data)).toThrow(QueueLiteError);
    expect(calls).toBe(0);
    expect(instance.getStats().total).toBe(0);
    expect(instance.add('task', Object.setPrototypeOf([1, null], null)).data).toEqual([1, null]);
  });
  it('validates required options, scheduling, worker intervals and IDs', () => {
    expect(() => createQueue(undefined!)).toThrow(QueueLiteError);
    expect(() => createQueue({ database: '' })).toThrow(QueueLiteError);
    expect(() => createQueue({ database: 'bad\0path' })).toThrow(QueueLiteError);
    expect(() => createQueue({ database: join(database(), 'missing.db') })).toThrow('Could not open');
    const instance = queue();
    for (const runAt of [-1, 1.5, NaN, Infinity]) expect(() => instance.add('task', {}, { runAt })).toThrow();
    for (const pollIntervalMs of [0, -1, 1.5, Infinity, 2 ** 31]) expect(() => instance.createWorker({ pollIntervalMs })).toThrow();
    expect(() => instance.add('task', {}, null!)).toThrow();
    expect(() => instance.add('task', {}, { runAt: null! })).toThrow();
    expect(() => instance.createWorker(null!)).toThrow();
    expect(() => instance.createWorker({ pollIntervalMs: null! })).toThrow();
    expect(() => instance.getJob('')).toThrow();
    const worker = instance.createWorker();
    expect(() => worker.register('task', null!)).toThrow();
    worker.register('task', () => {});
    expect(() => worker.register('task', () => {})).toThrow('already registered');
  });
  it('validates state transitions and prevents terminal re-execution', () => {
    const storage = new Storage(database());
    try {
      const job = storage.add('task', '{}', { runAt: 0 });
      expect(storage.finish(job.id, 'unowned', null)).toBe(false);
      const claim = storage.claim()!;
      expect(claim.job.id).toBe(job.id);
      expect(storage.finish(job.id, 'unowned', null)).toBe(false);
      expect(storage.finish(job.id, claim.token, null)).toBe(true);
      expect(storage.claim()).toBeUndefined();
      expect(storage.finish(job.id, claim.token, null)).toBe(false);
    } finally { storage.close(); }
  });
});

describe('worker', () => {
  it('finishes the claimed job even when the handler mutates its snapshot ID', async () => {
    const instance = queue();
    const job = instance.add('task', {});
    const worker = instance.createWorker({ pollIntervalMs: 5 }).register('task', (snapshot) => { snapshot.id = 'changed'; });
    const running = worker.start();
    // Attach the rejection handler immediately so an engine error cannot be overlooked.
    const outcome = running.catch((error: unknown) => error);
    try {
      const result = await Promise.race([terminal(instance, job.id), outcome]);
      expect(result).toMatchObject({ id: job.id, status: 'completed' });
    } finally { await worker.stop(); await outcome; }
  });
  it('awaits async handlers, completes jobs and picks up newly added jobs', async () => {
    const instance = queue();
    const executed: string[] = [];
    const worker = instance.createWorker({ pollIntervalMs: 5 });
    worker.register('task', async (job) => { await setTimeout(10); executed.push(job.id); });
    const running = worker.start();
    const first = instance.add('task', { hello: 'world' });
    const finished = await terminal(instance, first.id);
    const second = instance.add('task', 123);
    await terminal(instance, second.id);
    await worker.stop(); await running;
    expect(executed).toEqual([first.id, second.id]);
    expect(finished).toMatchObject({ status: 'completed', attempts: 1, error: null });
    expect(finished.startedAt).toBeGreaterThanOrEqual(first.createdAt);
    expect(finished.finishedAt).toBeGreaterThanOrEqual(finished.startedAt!);
    expect(finished.updatedAt).toBe(finished.finishedAt);
  });
  it('records failures, unknown handlers and non-Error throws while continuing', async () => {
    const instance = queue();
    const worker = instance.createWorker({ pollIntervalMs: 5 });
    worker.register('bad', () => { throw new Error('Delivery failed'); });
    worker.register('odd', () => { throw Object.create(null); });
    worker.register('ok', () => {});
    const jobs = ['bad', 'missing', 'odd', 'ok'].map((name) => instance.add(name, {}));
    const running = worker.start();
    const results = await Promise.all(jobs.map((job) => terminal(instance, job.id)));
    await worker.stop(); await running;
    expect(results.map((job) => job.status)).toEqual(['failed', 'failed', 'failed', 'completed']);
    expect(results[0]?.error).toContain('Delivery failed');
    expect(results[1]?.error).toContain('No handler registered');
    expect(results[2]?.error).toContain('unprintable');
    expect(results.every((job) => job.attempts === 1 && job.finishedAt !== null)).toBe(true);
  });
  it('honors runAt without claiming jobs early', async () => {
    const instance = queue();
    const due = Date.now() + 150;
    const job = instance.add('task', {}, { runAt: due });
    const worker = instance.createWorker({ pollIntervalMs: 5 }).register('task', () => {});
    const running = worker.start();
    await setTimeout(30);
    expect(instance.getJob(job.id)?.status).toBe('pending');
    const result = await terminal(instance, job.id);
    await worker.stop(); await running;
    expect(result.startedAt).toBeGreaterThanOrEqual(due);
  });
  it('stops gracefully, does not claim more work, and restarts', async () => {
    const instance = queue();
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const first = instance.add('task', {}, { runAt: 0 });
    const second = instance.add('task', {}, { runAt: 1 });
    const worker = instance.createWorker().register('task', () => gate);
    const running = worker.start();
    expect(() => worker.start()).toThrow('already running');
    await setTimeout(0);
    expect(instance.getJob(first.id)?.status).toBe('active');
    const stopped = worker.stop();
    release();
    await stopped; await running;
    expect(instance.getJob(first.id)?.status).toBe('completed');
    expect(instance.getJob(second.id)?.status).toBe('pending');
    const restarted = worker.start();
    await terminal(instance, second.id);
    await worker.stop(); await restarted;
  });
  it('closes safely while a handler is active and rejects further use', async () => {
    const path = database();
    const instance = queue(path);
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const job = instance.add('task', {});
    const worker = instance.createWorker().register('task', () => gate);
    const running = worker.start();
    await setTimeout(0);
    expect(instance.getJob(job.id)?.status).toBe('active');
    const closing = instance.close();
    expect(instance.close()).toBe(closing);
    expect(() => instance.add('task', {})).toThrow('closed');
    expect(() => worker.start()).toThrow('closed');
    expect(() => instance.createWorker()).toThrow('closed');
    release(); await closing; await running;
    expect(() => instance.getJob(job.id)).toThrow('closed');
    expect(queue(path).getJob(job.id)?.status).toBe('completed');
  });
  it('waits for completion when the first synchronous handler requests a stop', async () => {
    const instance = queue();
    const job = instance.add('task', {});
    let release!: () => void;
    let stopped = false;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    let stopping: Promise<void> | undefined;
    const worker = instance.createWorker().register('task', () => {
      stopping = worker.stop().then(() => { stopped = true; });
      return gate;
    });
    const running = worker.start();
    try {
      await setTimeout(0);
      expect(stopping).toBeDefined();
      expect(stopped).toBe(false);
    } finally { release(); await running; await stopping; }
    expect(instance.getJob(job.id)?.status).toBe('completed');
  });
  it('exposes engine errors through the start promise', async () => {
    const storage = new Storage(database());
    const worker = new Worker(storage, () => {});
    storage.close();
    await expect(worker.start()).rejects.toThrow();
    await worker.stop();
  });
  it('does not hold write transactions open during a handler', async () => {
    const path = database();
    const instance = queue(path);
    const other = queue(path);
    const original = instance.add('task', {});
    let added: Job | undefined;
    const worker = instance.createWorker().register('task', async () => {
      added = other.add('later', {}, { runAt: Date.now() + 60000 });
      await setTimeout(5);
    });
    const running = worker.start();
    expect((await terminal(instance, original.id)).status).toBe('completed');
    await worker.stop(); await running;
    expect(added?.status).toBe('pending');
  });
  it('wakes promptly from a long poll when stopped', async () => {
    const worker = queue().createWorker({ pollIntervalMs: 60000 });
    const running = worker.start();
    await setTimeout(0);
    const start = Date.now();
    await worker.stop(); await running;
    expect(Date.now() - start).toBeLessThan(1000);
  });
  it('claims atomically across independent database instances', async () => {
    const path = database();
    const first = queue(path); const second = queue(path);
    const jobs = Array.from({ length: 30 }, () => first.add('task', {}));
    const seen: string[] = [];
    const workers = [first, second].map((instance) => instance.createWorker({ pollIntervalMs: 5 }).register('task', async (job) => {
      seen.push(job.id); await setTimeout(2);
    }));
    const running = workers.map((worker) => worker.start());
    await Promise.all(jobs.map((job) => terminal(first, job.id)));
    await Promise.all(workers.map((worker) => worker.stop())); await Promise.all(running);
    expect(seen).toHaveLength(30); expect(new Set(seen).size).toBe(30);
    expect(jobs.every((job) => first.getJob(job.id)?.attempts === 1)).toBe(true);
  });
  it('claims atomically across competing operating system processes', async () => {
    const path = database();
    const instance = queue(path);
    const ids = Array.from({ length: 40 }, () => instance.add('task', {}).id);
    const execute = promisify(execFile);
    const fixture = fileURLToPath(new URL('./fixtures/process.mjs', import.meta.url));
    const results = await Promise.all([0, 1].map(() => execute(process.execPath, [fixture, 'race', path, ...ids], { timeout: 15000 })));
    const claims = results.map((result) => JSON.parse(result.stdout) as string[]);
    const all = claims.flat();
    expect(claims.every((items) => items.length > 0)).toBe(true);
    expect(all).toHaveLength(ids.length); expect(new Set(all).size).toBe(ids.length);
    expect(ids.every((id) => instance.getJob(id)?.attempts === 1)).toBe(true);
  }, 20000);
});
