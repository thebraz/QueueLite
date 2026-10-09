import { afterEach, describe, expect, it } from 'vitest';
import { spawn } from 'node:child_process';
import type { ChildProcess } from 'node:child_process';
import { once } from 'node:events';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as sleep } from 'node:timers/promises';
import Database from 'better-sqlite3';
import { createQueue } from '../src/index.js';
import type { Job, Queue } from '../src/index.js';
import { isBusy } from '../src/reliability.js';

const children = new Set<ChildProcess>();
const directories: string[] = [];
const queues: Queue[] = [];
interface Message { type: string; id?: string; attempt?: number; ids?: string[]; seen?: string[]; jobs?: Job[] }
function queue(): Queue {
  const directory = mkdtempSync(join(tmpdir(), 'queuelite-process-')); directories.push(directory);
  const q = createQueue({ database: join(directory, 'jobs.db') }); queues.push(q); return q;
}
function database(): string { return join(directories.at(-1)!, 'jobs.db'); }
function launch(mode: string, path: string, ...ids: string[]): { child: ChildProcess; receive: (type: string) => Promise<Message> } {
  const child = spawn(process.execPath, [fileURLToPath(new URL('./fixtures/reliability-process.mjs', import.meta.url)), mode, path, ...ids], {
    stdio: ['ignore', 'ignore', 'pipe', 'ipc'],
  });
  children.add(child);
  const messages: Message[] = [];
  const waiters = new Set<{ type: string; resolve: (value: Message) => void; reject: (error: Error) => void }>();
  let stderr = '';
  child.stderr!.on('data', (data: Buffer) => { stderr = (stderr + data.toString()).slice(-4000); });
  child.on('message', (message: Message) => {
    const waiter = [...waiters].find((w) => w.type === message.type);
    if (waiter) { waiters.delete(waiter); waiter.resolve(message); } else messages.push(message);
  });
  const rejectWaiters = (error: Error): void => { for (const w of waiters) w.reject(error); waiters.clear(); };
  child.on('error', rejectWaiters);
  child.on('exit', (code, signal) => {
    children.delete(child); rejectWaiters(new Error(`Fixture exited (${code ?? signal}): ${stderr}`));
  });
  return { child, receive: (type) => {
    const index = messages.findIndex((m) => m.type === type);
    if (index >= 0) return Promise.resolve(messages.splice(index, 1)[0]!);
    return new Promise<Message>((resolve, reject) => {
      if (child.exitCode !== null || child.signalCode !== null) return reject(new Error(`Fixture already exited: ${stderr}`));
      const waiter = { type, resolve, reject }; waiters.add(waiter);
      const timer = globalThis.setTimeout(() => { waiters.delete(waiter); reject(new Error(`No ${type} message: ${stderr}`)); }, 10000);
      waiter.resolve = (value) => { clearTimeout(timer); resolve(value); };
      waiter.reject = (error) => { clearTimeout(timer); reject(error); };
    });
  } };
}
async function exit(child: ChildProcess): Promise<void> {
  if (child.exitCode === null && child.signalCode === null) await once(child, 'exit');
}
async function until(check: () => boolean): Promise<void> {
  for (let i = 0; i < 500; i++) { if (check()) return; await sleep(5); }
  throw new Error('Condition timed out.');
}
afterEach(async () => {
  for (const child of [...children]) { child.kill('SIGKILL'); await exit(child); }
  await Promise.all(queues.splice(0).map((q) => q.close()));
  for (const directory of directories.splice(0)) rmSync(directory, { recursive: true, force: true });
});

describe('separate-process failure and recovery', () => {
  it('deduplicates simultaneous producers at the SQLite uniqueness boundary', async () => {
    const q = queue(); const path = database();
    const producers = [launch('produce', path), launch('produce', path), launch('produce', path)];
    await Promise.all(producers.map((p) => p.receive('ready')));
    for (const p of producers) p.child.send('go');
    const results = await Promise.all(producers.map((p) => p.receive('produced')));
    await Promise.all(producers.map((p) => exit(p.child)));
    const ids = results.flatMap((r) => r.ids!);
    expect(ids).toHaveLength(90); expect(new Set(ids).size).toBe(1);
    expect(q.getJob(ids[0]!)?.idempotencyKey).toBe('concurrent-key');
    const db = new Database(path);
    try { expect(db.prepare('SELECT count(*) AS count FROM jobs').get()).toEqual({ count: 1 }); }
    finally { db.close(); }
  }, 20000);
  it('recovers all active claims after a killed worker process, preserving history and budgets', async () => {
    const q = queue(); const path = database(); const jobs = Array.from({ length: 3 }, () => q.add('task', {}, { attempts: 3 }));
    const crashed = launch('crash', path, ...jobs.map((j) => j.id));
    await Promise.all(jobs.map(() => crashed.receive('claimed')));
    crashed.child.kill('SIGKILL'); await exit(crashed.child);
    const recovered = launch('recover', path, ...jobs.map((j) => j.id));
    const result = await recovered.receive('done'); await exit(recovered.child);
    expect(new Set(result.seen).size).toBe(3);
    expect(result.jobs?.every((j) => j.status === 'completed' && j.attempts === 2 && j.errorHistory[0]?.kind === 'lease-expired')).toBe(true);
    const db = new Database(path);
    try { expect(db.pragma('integrity_check')).toEqual([{ integrity_check: 'ok' }]); }
    finally { db.close(); }
  }, 20000);
  it('permanently fails an exhausted crashed claim instead of replaying it indefinitely', async () => {
    const q = queue(); const path = database(); const job = q.add('task', {}, { attempts: 1 });
    const crashed = launch('crash', path, job.id); await crashed.receive('claimed');
    crashed.child.kill('SIGKILL'); await exit(crashed.child);
    const recovered = launch('recover', path, job.id); const result = await recovered.receive('done'); await exit(recovered.child);
    expect(result.seen).toEqual([]); expect(result.jobs?.[0]).toMatchObject({ status: 'failed', attempts: 1, errorHistory: [expect.objectContaining({ kind: 'lease-expired' })] });
  }, 20000);
  it('fences a live but stalled process when another process recovers and completes its job', async () => {
    const q = queue(); const path = database(); const job = q.add('task', {}, { attempts: 3 });
    const stale = launch('stale', path, job.id); await stale.receive('ready');
    stale.child.send('stall'); await stale.receive('stalled');
    const recovered = launch('recover', path, job.id); const current = await recovered.receive('done'); await exit(recovered.child);
    expect(current.jobs?.[0]).toMatchObject({ status: 'completed', attempts: 2 });
    const committed = q.getJob(job.id);
    await stale.receive('done'); await exit(stale.child);
    expect(q.getJob(job.id)).toEqual(committed);
  }, 20000);
});

describe('SQLite contention', () => {
  it('yields while another process holds a write lock, then claims after release', async () => {
    const q = queue(); const path = database(); const job = q.add('task', {});
    const locker = launch('lock', path); await locker.receive('locked');
    let executed = 0;
    const w = q.createWorker({ pollIntervalMs: 5 }).register('task', () => { executed++; });
    const running = w.start();
    const start = Date.now(); await sleep(50);
    expect(Date.now() - start).toBeLessThan(1000); expect(q.getJob(job.id)?.status).toBe('pending'); expect(executed).toBe(0);
    expect(() => q.add('other', {})).toThrow(expect.objectContaining({ code: 'SQLITE_BUSY' }));
    locker.child.send('release'); await locker.receive('released'); await exit(locker.child);
    await until(() => q.getJob(job.id)?.status === 'completed'); await w.stop(); await running;
    expect(executed).toBe(1);
  }, 20000);
  it('retries a contested terminal write without rerunning the successful handler', async () => {
    const q = queue(); const path = database(); const job = q.add('task', {}, { attempts: 2 });
    let release!: () => void; let executed = 0;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const w = q.createWorker({ pollIntervalMs: 5, leaseDurationMs: 2000, heartbeatIntervalMs: 50 }).register('task', async () => { executed++; await gate; });
    const running = w.start(); await until(() => executed === 1);
    const locker = launch('lock', path); await locker.receive('locked'); release();
    await sleep(80); expect(q.getJob(job.id)?.status).toBe('active');
    locker.child.send('release'); await locker.receive('released'); await exit(locker.child);
    await until(() => q.getJob(job.id)?.status === 'completed'); await w.stop(); await running;
    expect(q.getJob(job.id)).toMatchObject({ attempts: 1, errorHistory: [] }); expect(executed).toBe(1);
  }, 20000);
  it('loses an expired lease under long contention and recovers without allowing a stale write', async () => {
    const q = queue(); const path = database(); const job = q.add('task', {}, { attempts: 2 });
    let release!: () => void; let signal!: AbortSignal;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const w = q.createWorker({ pollIntervalMs: 5, leaseDurationMs: 250, heartbeatIntervalMs: 50 }).register('task', async (_, ctx) => { signal = ctx.signal; await gate; });
    const running = w.start(); await until(() => signal !== undefined);
    const locker = launch('lock', path); await locker.receive('locked'); await sleep(300);
    locker.child.send('release'); await locker.receive('released'); await exit(locker.child);
    await until(() => signal.aborted); await w.stop({ timeoutMs: 0 }).catch((error: Error) => { expect(error.message).toContain('deadline'); }); await running;
    const recovered = launch('recover', path, job.id); const result = await recovered.receive('done'); await exit(recovered.child);
    expect(result.jobs?.[0]).toMatchObject({ status: 'completed', attempts: 2 });
    const snapshot = q.getJob(job.id); release(); await sleep(10); expect(q.getJob(job.id)).toEqual(snapshot);
  }, 20000);
  it('classifies only SQLite busy and lock errors as retryable engine contention', () => {
    expect(isBusy({ code: 'SQLITE_BUSY_SNAPSHOT' })).toBe(true); expect(isBusy({ code: 'SQLITE_LOCKED' })).toBe(true);
    expect(isBusy(new Error('SQLITE_BUSY'))).toBe(false); expect(isBusy({ code: 'SQLITE_CORRUPT' })).toBe(false); expect(isBusy(null)).toBe(false);
  });
});
