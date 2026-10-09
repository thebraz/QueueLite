import { afterEach, describe, expect, it, vi } from 'vitest';
import { execFile } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import { createQueue, diagnose, QueueLiteError } from '../src/index.js';
import type { LifecycleEvent, ListOptions, Queue, QueueOptions } from '../src/index.js';
import { Storage } from '../src/storage.js';

const directories: string[] = [];
const queues: Queue[] = [];
const stores: Storage[] = [];
const cliPath = fileURLToPath(new URL('../dist/cli.js', import.meta.url));
function path(): string {
  const directory = mkdtempSync(join(tmpdir(), 'queuelite-inspection-')); directories.push(directory);
  return join(directory, 'jobs.db');
}
function open(database = path(), options: Partial<QueueOptions> = {}): Queue {
  const queue = createQueue({ database, ...options }); queues.push(queue); return queue;
}
function cli(...args: string[]): Promise<{ code: number; stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    execFile(process.execPath, [cliPath, ...args], { encoding: 'utf8', timeout: 10000, windowsHide: true }, (error, stdout, stderr) => {
      if (error && typeof error.code !== 'number') { reject(error); return; }
      resolve({ code: error?.code as number ?? 0, stdout, stderr });
    });
  });
}
function hash(file: string): string { return createHash('sha256').update(readFileSync(file)).digest('hex'); }
async function fixture() {
  const database = path(); const queue = open(database);
  const failed = queue.add('fail', { secret: 'payload-canary' });
  const completed = queue.add('ok', {});
  await queue.createWorker({ pollIntervalMs: 2 }).register('ok', () => {}).register('fail', () => {
    throw new Error('unlabelled-private-canary token=credential-canary');
  }).drain();
  const pending = queue.add('ok', {});
  const delayed = queue.add('ok', {}, { delay: 60000 });
  return { database, queue, failed, completed, pending, delayed };
}
afterEach(async () => {
  vi.useRealTimers(); vi.restoreAllMocks();
  await Promise.all(queues.splice(0).map((queue) => queue.close()));
  for (const store of stores.splice(0)) store.close();
  for (const directory of directories.splice(0)) rmSync(directory, { recursive: true, force: true });
});

describe('bounded public inspection', () => {
  it('uses stable cursors across pages and inserts, without returning payloads or failure histories', () => {
    const queue = open();
    const jobs = Array.from({ length: 5 }, () => queue.add('task', { private: 'canary' }));
    const first = queue.listJobs({ limit: 2 });
    const last = queue.add('task', {});
    const second = queue.listJobs({ limit: 2, after: first.nextCursor! });
    const third = queue.listJobs({ limit: 2, after: second.nextCursor! });
    expect([...first.items, ...second.items, ...third.items].map((job) => job.id)).toEqual([...jobs, last].map((job) => job.id));
    expect(third.nextCursor).toBeNull();
    expect(JSON.stringify(first)).not.toMatch(/canary|errorHistory|idempotencyKey|"data"/);
    expect(queue.getJobSummary(jobs[0]!.id)).toEqual(first.items[0]);
    expect(queue.getJobSummary('missing')).toBeUndefined();
    expect(queue.listJobs({ after: Number.MAX_SAFE_INTEGER })).toEqual({ items: [], nextCursor: null });
  });
  it('filters all persisted statuses and delayed jobs, and aggregates statistics', async () => {
    const { queue, failed, completed, pending, delayed } = await fixture();
    const cancelled = queue.cancelJob(pending.id);
    expect(queue.listJobs({ status: 'failed' }).items[0]?.id).toBe(failed.id);
    expect(queue.listJobs({ status: 'completed' }).items[0]?.id).toBe(completed.id);
    expect(queue.listJobs({ status: 'cancelled' }).items[0]?.id).toBe(cancelled.id);
    expect(queue.listJobs({ status: 'delayed' }).items[0]?.id).toBe(delayed.id);
    expect(queue.listJobs({ status: 'pending' }).items).toHaveLength(1);
    expect(queue.getStats()).toMatchObject({ total: 4, pending: 0, delayed: 1, active: 0, failed: 1, completed: 1,
      cancelled: 1, retryAttempts: 0, outcomes: { completed: 1, failed: 1, recovered: 0 }, averageDurationMs: expect.any(Number) });
  });
  it('returns defined zero statistics for an empty queue', () => {
    expect(open().getStats()).toEqual({ total: 0, pending: 0, delayed: 0, active: 0, completed: 0, failed: 0,
      cancelled: 0, retryAttempts: 0, outcomes: { completed: 0, failed: 0, recovered: 0 }, averageDurationMs: null });
  });
  it('persists attempt identity across manual retry cycles and paginates failures separately', async () => {
    const { database, queue, failed } = await fixture();
    const worker = queue.createWorker({ pollIntervalMs: 2 }).register('fail', () => { throw new Error('password=secret-canary'); }).register('ok', () => {});
    queue.retryJob(failed.id); await worker.drain();
    queue.retryJob(failed.id); await worker.drain();
    const first = queue.getFailureHistory(failed.id, { limit: 1 });
    const second = queue.getFailureHistory(failed.id, { limit: 1, after: first.nextCursor! });
    const third = queue.getFailureHistory(failed.id, { limit: 1, after: second.nextCursor! });
    expect(third.nextCursor).toBeNull();
    expect(new Set([first.items[0]!.id, second.items[0]!.id, third.items[0]!.id]).size).toBe(3);
    expect([first, second, third].flatMap((page) => page.items.map((attempt) => attempt.attempt))).toEqual([1, 1, 1]);
    expect(JSON.stringify(queue.getFailureHistory(failed.id))).not.toContain('secret-canary');
    expect(queue.getStats().retryAttempts).toBe(2);
    await queue.close();
    expect(open(database).getAttempts(failed.id).items).toHaveLength(3);
  });
  it('inspects claims without exposing ownership tokens, and fences non-owner transitions', () => {
    const database = path(); const queue = open(database);
    const store = new Storage(database); stores.push(store);
    const job = queue.add('task', {}); const claim = store.claim()!;
    expect(queue.getActiveClaims().items).toMatchObject([{ id: job.id, status: 'active', leaseExpiresAt: expect.any(Number) }]);
    expect(JSON.stringify(queue.getActiveClaims())).not.toContain(claim.token);
    expect(queue.getAttempts(job.id).items).toMatchObject([{ outcome: 'active', finishedAt: null }]);
    expect(() => queue.retryJob(job.id)).toThrow('Only failed');
    expect(() => queue.cancelJob(job.id)).toThrow('Only pending');
    expect(store.finish(job.id, 'wrong-owner', null)).toBe(false);
    expect(queue.getStats().active).toBe(1);
  });
  it.each([{ limit: 0 }, { limit: 1001 }, { limit: 1.5 }, { after: -1 }, { after: NaN }, { status: 'invalid' }, null])('validates listing options %#', (options) => {
    expect(() => open().listJobs(options as ListOptions)).toThrow(QueueLiteError);
  });
  it('validates history, hook and read-only options, and rejects operations after close', async () => {
    const database = path(); const queue = open(database);
    expect(() => queue.getAttempts('', {})).toThrow();
    expect(() => queue.getFailureHistory('job', { limit: 1001 })).toThrow();
    expect(() => queue.getActiveClaims(null!)).toThrow();
    expect(() => open(database, { onEvent: 1 as never })).toThrow('onEvent');
    expect(() => open(database, { logger: {} as never })).toThrow('logger');
    expect(() => open(database, { readOnly: 'yes' as never })).toThrow('boolean');
    const reader = open(database, { readOnly: true });
    expect(reader.getStats().total).toBe(0);
    expect(() => reader.createWorker()).toThrow('read-only');
    expect(() => reader.add('task', {})).toThrow('read-only');
    await queue.close();
    expect(() => queue.getStats()).toThrow('closed');
    expect(() => queue.listJobs()).toThrow('closed');
    expect(() => queue.getFailureHistory('job')).toThrow('closed');
  });
  it('reports transient contention with safe retry instructions', () => {
    const database = path(); const queue = open(database); const lock = new Database(database);
    lock.exec('BEGIN IMMEDIATE');
    try {
      expect(() => queue.add('task', { token: 'payload-canary' })).toThrow('bounded asynchronous backoff');
      try { queue.add('task', {}); } catch (error) { expect(error).toMatchObject({ code: 'SQLITE_BUSY', retryable: true }); }
    } finally { lock.exec('ROLLBACK'); lock.close(); }
  });
});

describe('lifecycle and structured logging', () => {
  it('emits committed transitions once, without arbitrary names, payloads or errors', async () => {
    const events: LifecycleEvent[] = []; const logs: LifecycleEvent[] = [];
    const queue = open(path(), { onEvent: (event) => {
      expect(queue.getJobSummary(event.jobId)?.status).toBe(event.status);
      events.push(event);
    }, logger: { info: (event) => { logs.push(event); } } });
    let calls = 0;
    const job = queue.add('name-private-canary', { token: 'payload-private-canary' }, { attempts: 2, idempotencyKey: 'key-private-canary' });
    queue.add('name-private-canary', {}, { idempotencyKey: 'key-private-canary' });
    const worker = queue.createWorker({ pollIntervalMs: 2 }).register('name-private-canary', () => {
      if (++calls === 1) throw new Error('unlabelled-error-private-canary');
    });
    await worker.drain();
    expect(events.map((event) => event.type)).toEqual(['enqueued', 'started', 'failed', 'retried', 'started', 'completed']);
    expect(events).toEqual(logs);
    expect(JSON.stringify(logs)).not.toContain('private-canary');
    expect(events.at(-1)).toMatchObject({ jobId: job.id, durationMs: expect.any(Number) });
  });
  it('isolates throwing and rejecting observer hooks from job execution', async () => {
    const queue = open(path(), { onEvent: async () => { throw new Error('Observer failed'); }, logger: { info: () => { throw new Error('Logger failed'); } } });
    const job = queue.add('task', {});
    await queue.createWorker().register('task', () => {}).drain();
    expect(queue.getJob(job.id)?.status).toBe('completed');
    expect(queue.getAttempts(job.id).items).toHaveLength(1);
  });
  it('records recovery and approximate durations without accepting stale completions', () => {
    vi.useFakeTimers(); vi.setSystemTime(1000);
    const events: LifecycleEvent[] = [];
    const database = path(); const queue = open(database);
    const store = new Storage(database, { onEvent: (event) => { events.push(event); } }); stores.push(store);
    const job = queue.add('task', {}, { attempts: 2 });
    const old = store.claim(10)!;
    vi.setSystemTime(1020);
    const replacement = store.claim(100)!;
    expect(store.finish(job.id, old.token, null)).toBe(false);
    vi.setSystemTime(1030); expect(store.finish(job.id, replacement.token, null)).toBe(true);
    expect(events.map((event) => event.type)).toEqual(['started', 'failed', 'recovered', 'retried', 'started', 'completed']);
    expect(queue.getStats()).toMatchObject({ retryAttempts: 1, outcomes: { completed: 1, failed: 0, recovered: 1 }, averageDurationMs: 15 });
    expect(queue.getFailureHistory(job.id).items[0]?.error?.kind).toBe('lease-expired');
  });
  it('does not emit events for a rolled back transition', () => {
    const events: LifecycleEvent[] = [];
    const database = path(); const queue = open(database, { onEvent: (event) => { events.push(event); } });
    queue.add('task', {}); events.length = 0;
    const db = new Database(database);
    db.exec("CREATE TRIGGER reject_attempt BEFORE INSERT ON job_attempts BEGIN SELECT RAISE(ABORT, 'fixture'); END;");
    db.close();
    const store = new Storage(database, { onEvent: (event) => { events.push(event); } }); stores.push(store);
    expect(() => store.claim()).toThrow();
    expect(queue.getStats()).toMatchObject({ pending: 1, active: 0 });
    expect(events).toEqual([]);
  });
});

describe('diagnostics and migration', () => {
  it('does not create missing storage, and validates configuration', () => {
    const database = path();
    expect(diagnose({ database }).ok).toBe(false); expect(existsSync(database)).toBe(false);
    expect(diagnose({ database: ':memory:' }).ok).toBe(false);
    expect(diagnose({ database: '' }).checks[0]?.name).toBe('configuration');
  });
  it('reports a healthy database without changing its contents', async () => {
    const database = path(); const queue = open(database); queue.add('task', {}); await queue.close();
    const before = hash(database);
    expect(diagnose({ database })).toMatchObject({ ok: true, schemaVersion: 3 });
    expect(hash(database)).toBe(before);
  });
  it('reports expired claims without recovery and suggests an explicit worker restart', () => {
    vi.useFakeTimers(); vi.setSystemTime(1000);
    const database = path(); const queue = open(database); const job = queue.add('task', {});
    const store = new Storage(database); stores.push(store); store.claim(10); vi.setSystemTime(1020);
    const result = diagnose({ database });
    expect(result.ok).toBe(false);
    expect(result.checks.find((check) => check.name === 'leases')).toMatchObject({ status: 'warning', message: expect.stringContaining('Start a worker') });
    expect(queue.getJob(job.id)?.status).toBe('active');
    expect(queue.getFailureHistory(job.id).items).toEqual([]);
  });
  it('detects inconsistent states and missing schema despite a current version number', () => {
    const database = path(); const queue = open(database); const job = queue.add('task', {});
    const db = new Database(database);
    db.prepare('UPDATE jobs SET attempts = max_attempts WHERE id = ?').run(job.id); db.close();
    expect(diagnose({ database }).checks.find((check) => check.name === 'state')?.status).toBe('error');
    const incompatible = path(); const empty = new Database(incompatible); empty.pragma('user_version = 3'); empty.close();
    expect(diagnose({ database: incompatible }).ok).toBe(false);
    const before = hash(incompatible);
    expect(() => open(incompatible)).toThrow('initialize');
    expect(hash(incompatible)).toBe(before);
  });
  it('migrates version 2 history transactionally, while diagnostics and read-only access never migrate', async () => {
    const { database, queue, failed, completed, pending } = await fixture();
    const previousFailure = queue.getJob(failed.id); await queue.close();
    const db = new Database(database);
    db.exec('DROP TABLE job_attempts; DROP INDEX jobs_status_seq; DROP INDEX jobs_schedule; PRAGMA user_version = 2;'); db.close();
    const before = hash(database);
    expect(diagnose({ database })).toMatchObject({ ok: false, schemaVersion: 2 });
    expect(() => open(database, { readOnly: true })).toThrow('upgrade required');
    for (const command of ['retry', 'cancel']) {
      expect((await cli(command, pending.id, '--db', database, '--json')).code).toBe(1);
    }
    expect(hash(database)).toBe(before);
    const migrated = open(database);
    expect(migrated.getJob(failed.id)).toEqual(previousFailure);
    expect(migrated.getJob(pending.id)).toEqual(pending);
    expect(migrated.getFailureHistory(failed.id).items).toHaveLength(1);
    expect(migrated.getAttempts(completed.id).items[0]?.outcome).toBe('completed');
    expect(diagnose({ database }).ok).toBe(true);
  });
  it('rolls back a failed version 2 upgrade without losing jobs or changing the version', async () => {
    const database = path(); const queue = open(database); const job = queue.add('task', {}); await queue.close();
    const db = new Database(database);
    db.exec('DROP TABLE job_attempts; DROP INDEX jobs_status_seq; DROP INDEX jobs_schedule; CREATE TABLE job_attempts (sentinel TEXT); PRAGMA user_version = 2;'); db.close();
    expect(() => open(database)).toThrow('initialize');
    const check = new Database(database);
    try {
      expect(check.pragma('user_version', { simple: true })).toBe(2);
      expect(check.prepare('SELECT id FROM jobs').get()).toEqual({ id: job.id });
      expect(check.prepare("SELECT name FROM sqlite_master WHERE name = 'jobs_status_seq'").get()).toBeUndefined();
    } finally { check.close(); }
  });
});

describe('public CLI', () => {
  it.each(['stats', 'list', 'inspect', 'retry', 'cancel', 'doctor'].flatMap((command) => [false, true].map((json) => ({ command, json }))))('runs $command with json=$json', async ({ command, json }) => {
    const { database, queue, failed, pending } = await fixture();
    const id = command === 'cancel' ? pending.id : failed.id;
    const result = await cli(command, ...(['inspect', 'retry', 'cancel'].includes(command) ? [id] : []), '--db', database, ...(json ? ['--json'] : []));
    expect(result.code).toBe(0); expect(result.stderr).toBe('');
    expect(result.stdout.trim()).not.toBe('');
    expect(result.stdout).not.toMatch(/payload-canary|credential-canary|unlabelled-private-canary|errorHistory|lease_token/);
    if (json) expect(JSON.parse(result.stdout)).toBeTypeOf('object');
    if (command === 'cancel') expect(queue.getJob(id)?.status).toBe('cancelled');
    if (command === 'retry') expect(queue.getJob(id)).toMatchObject({ status: 'pending', attempts: 0, errorHistory: [expect.any(Object)] });
  });
  it('paginates filtered list and inspect output using their emitted cursors', async () => {
    const { database, failed, pending, delayed } = await fixture();
    const first = JSON.parse((await cli('list', '--db', database, '--status', 'pending', '--limit', '1', '--json')).stdout);
    expect(first.items[0].id).toBe(pending.id);
    const second = JSON.parse((await cli('list', '--db', database, '--status', 'pending', '--limit', '1', '--after', String(first.nextCursor), '--json')).stdout);
    expect(second.items[0].id).toBe(delayed.id); expect(second.nextCursor).toBeNull();
    const inspection = JSON.parse((await cli('inspect', failed.id, '--db', database, '--limit', '1', '--json')).stdout);
    expect(inspection.attempts.items).toHaveLength(1); expect(inspection.failures.items[0].error.kind).toBe('handler');
    expect(inspection.job).not.toHaveProperty('data');
  });
  it.each([[], ['--help'], ['list', '--help'], ['-h'], ['--help', '--json']].map((args) => ({ args })))('shows help for $args without opening a database', async ({ args }) => {
    const result = await cli(...args); expect(result.code).toBe(0); expect(result.stdout).toContain('queuelite <command>');
  });
  it.each([
    ['unknown'], ['retry'], ['cancel'], ['inspect'], ['stats', 'extra'], ['retry', 'a', 'b'], ['--unknown'],
    ['list', '--limit', '0'], ['list', '--limit', '1001'], ['list', '--limit', '1.5'], ['list', '--limit', 'NaN'],
    ['list', '--after', '-1'], ['list', '--after', '9007199254740992'], ['list', '--status', 'deleted'],
    ['stats', '--status', 'failed'], ['doctor', '--limit', '1'], ['retry', 'job', '--after', '1'],
    ['stats', '--db'], ['list', '--db', ':memory:'], ['stats', '--db', ''],
  ].map((args) => ({ args })))('rejects invalid arguments $args as JSON', async ({ args }) => {
    const result = await cli(...args, '--json'); expect(result.code).toBe(2); expect(JSON.parse(result.stdout).error).toBeTypeOf('string');
  });
  it.each(['inspect', 'retry', 'cancel'])('returns not-found exit code for %s', async (command) => {
    const { database } = await fixture();
    const result = await cli(command, 'missing-id', '--db', database, '--json');
    expect(result.code).toBe(3); expect(JSON.parse(result.stdout).error).toContain('not found');
  });
  it('rejects incompatible transitions without losing job history', async () => {
    const { database, queue, completed, failed } = await fixture();
    expect((await cli('retry', completed.id, '--db', database, '--json')).code).toBe(1);
    expect((await cli('cancel', failed.id, '--db', database, '--json')).code).toBe(1);
    expect(queue.getJob(completed.id)?.status).toBe('completed');
    expect(queue.getFailureHistory(failed.id).items).toHaveLength(1);
  });
  it('does not create databases for any command on a missing path', async () => {
    const database = path();
    for (const command of ['stats', 'list', 'inspect', 'retry', 'cancel', 'doctor']) {
      expect((await cli(command, ...(['inspect', 'retry', 'cancel'].includes(command) ? ['job'] : []), '--db', database, '--json')).code).toBe(1);
      expect(existsSync(database)).toBe(false);
    }
  });
  it('escapes stored control characters in human output', async () => {
    const database = path(); open(database).add('task\u001b[31m\nnext', {});
    const result = await cli('list', '--db', database);
    expect(result.code).toBe(0); expect(result.stdout).not.toContain('\u001b');
    expect(result.stdout).toContain('\\u001b'); expect(result.stdout).toContain('\\nnext');
  });
  it.each(['corrupt', 'incompatible', 'uninitialized', 'directory'])('handles %s storage safely', async (kind) => {
    let database = path();
    if (kind === 'corrupt') writeFileSync(database, 'damaged-database-private-canary');
    else if (kind === 'incompatible' || kind === 'uninitialized') {
      const db = new Database(database); db.exec("CREATE TABLE sentinel(value TEXT); INSERT INTO sentinel VALUES ('keep');");
      db.pragma(`user_version = ${kind === 'incompatible' ? 99 : 0}`); db.close();
    } else database = join(database, '..');
    const before = kind === 'directory' ? null : hash(database);
    for (const command of ['stats', 'doctor', 'retry']) {
      const result = await cli(command, ...(command === 'retry' ? ['job'] : []), '--db', database, '--json');
      expect(result.code).toBe(1); expect(result.stdout).not.toContain('private-canary');
      if (before) expect(hash(database)).toBe(before);
    }
  });
});
