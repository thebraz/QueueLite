import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { isAbsolute, join, relative, resolve } from 'node:path';
import { performance } from 'node:perf_hooks';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import Database from 'better-sqlite3';
import { createQueue, diagnose, QueueLiteError } from '@thebraz/queuelite';
import { Storage } from '../dist/storage.js';

const root = fileURLToPath(new URL('..', import.meta.url));
const cliPath = join(root, 'dist', 'cli.js');
const statuses = ['pending', 'active', 'completed', 'failed', 'cancelled'];
const metadataKeys = ['id', 'name', 'status', 'attempts', 'maxAttempts', 'createdAt', 'updatedAt', 'runAt',
  'startedAt', 'finishedAt', 'priority', 'backoff', 'leaseExpiresAt'];

function owned(context, file) {
  const absolute = resolve(file);
  const part = relative(context.directory, absolute);
  assert.ok(part && !part.startsWith('..') && !isAbsolute(part), 'Verification may access only its own case directory.');
  return absolute;
}
function open(context, database = context.database, options = {}) {
  const queue = createQueue({ database: owned(context, database), ...options });
  context.queues.push(queue);
  for (const name of ['add', 'getJob', 'getJobSummary', 'listJobs', 'getStats', 'getAttempts', 'getFailureHistory',
    'getActiveClaims', 'retryJob', 'cancelJob', 'createWorker', 'close']) {
    assert.equal(typeof queue[name], 'function', `Missing required Stage 3 SDK method: ${name}.`);
  }
  return queue;
}
function sql(context, statement, parameters = [], database = context.database) {
  const db = new Database(owned(context, database), { readonly: true, fileMustExist: true });
  try { return db.prepare(statement).all(...parameters); } finally { db.close(); }
}
function fingerprint(context, database = context.database) {
  return createHash('sha256').update(readFileSync(owned(context, database))).digest('hex');
}
async function until(context, predicate, explanation, timeout = 10000) {
  const deadline = performance.now() + timeout;
  while (true) {
    if (context.errors.length) throw context.errors[0];
    const result = predicate();
    if (result) return result;
    if (performance.now() >= deadline) throw new Error(explanation);
    await sleep(10);
  }
}
function start(context, worker) {
  const running = worker.start();
  void running.catch((error) => context.errors.push(error));
  return running;
}
async function hold(context, queue, name = 'hold', options = {}) {
  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  context.release.push(release);
  const job = queue.add(name, {}, { attempts: 2 });
  let signal;
  const worker = queue.createWorker({ pollIntervalMs: 5, ...options }).register(name, async (_, execution) => {
    signal = execution.signal; await gate;
  });
  const running = start(context, worker);
  await until(context, () => signal && queue.getJob(job.id)?.status === 'active', 'The real worker did not claim its job.');
  return { job, worker, running, release, signal };
}
async function fixture(context) {
  const queue = open(context);
  const completed = queue.add('complete', {});
  const failed = queue.add('fail', { token: 'fixture-payload-canary' });
  await queue.createWorker({ pollIntervalMs: 5 }).register('complete', () => {}).register('fail', () => {
    throw new Error('Expected verification failure; token=fixture-error-canary');
  }).drain({ timeoutMs: 5000 });
  const active = await hold(context, queue);
  const pending = [queue.add('task', {}), queue.add('task', {})];
  const delayed = queue.add('task', {}, { delay: 600000 });
  const cancelled = queue.cancelJob(queue.add('task', {}).id);
  return { queue, completed, failed, active, pending, delayed, cancelled };
}
function execute(context, file, args, options = {}) {
  const began = performance.now();
  return new Promise((resolvePromise, reject) => {
    const child = execFile(process.execPath, [file, ...args], {
      cwd: options.cwd ?? context.directory, encoding: 'utf8', windowsHide: true,
      timeout: options.timeout ?? 15000, maxBuffer: 8 * 1024 * 1024, killSignal: 'SIGKILL',
      env: options.env ?? process.env,
    }, (error, stdout, stderr) => {
      const result = { pid: child.pid, code: error?.code ?? 0, elapsedMs: Math.round(performance.now() - began), stdout, stderr };
      context.evidence.processes ??= [];
      context.evidence.processes.push({ executable: relative(root, file), args, pid: result.pid, exitCode: result.code,
        elapsedMs: result.elapsedMs, stdoutBytes: Buffer.byteLength(stdout), stderrBytes: Buffer.byteLength(stderr),
        stdoutExcerpt: stdout.trim().slice(0, options.excerpt ?? 350), ...(stderr.trim() ? { stderrExcerpt: stderr.trim().slice(-1500) } : {}) });
      if (error && (typeof error.code !== 'number' || error.killed)) {
        reject(new Error(`Subprocess could not complete (${error.code ?? error.signal}): ${relative(root, file)}. ${stderr.slice(-1500)}`, { cause: error }));
      } else resolvePromise(result);
    });
  });
}
async function cli(context, args, { database = context.database, json = true, code = 0 } = {}) {
  const parameters = [...args];
  if (database !== null) parameters.push('--db', owned(context, database));
  if (json) parameters.push('--json');
  const result = await execute(context, cliPath, parameters);
  assert.equal(result.code, code, `Unexpected CLI exit code for ${args.join(' ')}: ${result.stdout} ${result.stderr}`);
  assert.equal(result.stderr, '', 'CLI emitted unexpected stderr.');
  assert.ok(result.stdout.trim(), 'CLI returned no output.');
  if (json) {
    try { result.data = JSON.parse(result.stdout); }
    catch (cause) { throw new Error(`CLI did not emit exactly one valid JSON value: ${args.join(' ')}`, { cause }); }
    assert.ok(result.data && typeof result.data === 'object' && !Array.isArray(result.data), 'JSON output must be an object.');
  }
  return result;
}
function keys(value, expected) { assert.deepEqual(Object.keys(value).sort(), [...expected].sort()); }
function page(value, limit = 100) {
  keys(value, ['items', 'nextCursor']);
  assert.ok(Array.isArray(value.items) && value.items.length <= limit);
  assert.ok(value.nextCursor === null || Number.isSafeInteger(value.nextCursor) && value.nextCursor > 0);
}
function metadata(value) {
  keys(value, metadataKeys);
  assert.equal(typeof value.id, 'string'); assert.equal(typeof value.name, 'string');
  assert.ok(statuses.includes(value.status));
  for (const key of ['createdAt', 'updatedAt', 'runAt', 'attempts', 'maxAttempts']) assert.ok(Number.isSafeInteger(value[key]) && value[key] >= 0, `Invalid metadata: ${key}`);
  for (const key of ['startedAt', 'finishedAt', 'leaseExpiresAt']) assert.ok(value[key] === null || Number.isSafeInteger(value[key]) && value[key] >= 0, `Invalid metadata: ${key}`);
  assert.ok(Number.isSafeInteger(value.priority));
  assert.ok(['fixed', 'exponential'].includes(value.backoff.type));
}
function attempts(value, limit = 100) {
  page(value, limit);
  for (const attempt of value.items) {
    keys(attempt, ['id', 'jobId', 'attempt', 'startedAt', 'finishedAt', 'outcome', 'retry', 'error']);
    assert.ok(Number.isSafeInteger(attempt.id) && attempt.id > 0);
    assert.ok(Number.isSafeInteger(attempt.attempt) && attempt.attempt > 0);
    assert.equal(typeof attempt.jobId, 'string'); assert.equal(typeof attempt.retry, 'boolean');
    assert.ok(['active', 'completed', 'failed', 'recovered'].includes(attempt.outcome));
    for (const key of ['startedAt', 'finishedAt']) assert.ok(attempt[key] === null || Number.isSafeInteger(attempt[key]));
    if (attempt.error !== null) {
      keys(attempt.error, ['kind', 'attempt', 'at']);
      assert.ok(['handler', 'missing-handler', 'lease-expired'].includes(attempt.error.kind));
      assert.equal(attempt.error.attempt, attempt.attempt); assert.ok(Number.isSafeInteger(attempt.error.at));
    }
  }
}
function diagnostics(value) {
  keys(value, ['ok', 'schemaVersion', 'checks']);
  assert.equal(typeof value.ok, 'boolean');
  assert.ok(value.schemaVersion === null || Number.isSafeInteger(value.schemaVersion));
  assert.ok(Array.isArray(value.checks) && value.checks.length > 0);
  for (const check of value.checks) {
    keys(check, ['name', 'status', 'message']);
    assert.ok(typeof check.name === 'string' && check.name.length > 0);
    assert.ok(['ok', 'warning', 'error'].includes(check.status));
    assert.ok(typeof check.message === 'string' && check.message.trim().length > 0);
  }
}
function stats(value) {
  keys(value, ['total', 'pending', 'delayed', 'active', 'completed', 'failed', 'cancelled', 'retryAttempts', 'outcomes', 'averageDurationMs']);
  for (const key of ['total', 'pending', 'delayed', 'active', 'completed', 'failed', 'cancelled', 'retryAttempts']) assert.ok(Number.isSafeInteger(value[key]) && value[key] >= 0, `Invalid statistic: ${key}`);
  keys(value.outcomes, ['completed', 'failed', 'recovered']);
  for (const count of Object.values(value.outcomes)) assert.ok(Number.isSafeInteger(count) && count >= 0);
  assert.ok(value.averageDurationMs === null || Number.isFinite(value.averageDurationMs) && value.averageDurationMs >= 0);
  assert.equal(value.total, value.pending + value.delayed + value.active + value.completed + value.failed + value.cancelled);
}

const cases = {
  '01': ['CLI help, command discovery and usage', async (context) => {
    for (const args of [[], ['--help'], ['-h'], ['inspect', '--help']]) {
      const result = await cli(context, args, { database: null, json: false });
      assert.match(result.stdout, /Usage: queuelite/);
      for (const command of ['stats', 'list', 'inspect', 'retry', 'cancel', 'doctor']) assert.match(result.stdout, new RegExp(`^  ${command}\\s`, 'm'), `Missing required CLI command: ${command}.`);
      for (const option of ['--db', '--status', '--limit', '--after', '--json', '--help']) assert.ok(result.stdout.includes(option), `Missing usage option: ${option}.`);
      assert.match(result.stdout, /Exit codes:/);
    }
    const result = await cli(context, ['--help'], { database: null }); keys(result.data, ['help']);
    assert.match(result.data.help, /Usage: queuelite/);
    assert.equal(existsSync(join(context.directory, 'queuelite.db')), false, 'Help created a database.');
  }],
  '02': ['Queue statistics compared with actual SQLite records', async (context) => {
    const { queue } = await fixture(context);
    const actual = (await cli(context, ['stats'])).data; stats(actual);
    const [counts] = sql(context, `SELECT count(*) AS total, sum(status = 'pending' AND run_at <= ?) AS pending,
      sum(status = 'pending' AND run_at > ?) AS delayed, sum(status = 'active') AS active,
      sum(status = 'completed') AS completed, sum(status = 'failed') AS failed, sum(status = 'cancelled') AS cancelled FROM jobs`, [Date.now(), Date.now()]);
    for (const [name, count] of Object.entries(counts)) assert.equal(actual[name], count, `CLI/SQLite mismatch: ${name}.`);
    assert.deepEqual(counts, { total: 7, pending: 2, delayed: 1, active: 1, completed: 1, failed: 1, cancelled: 1 });
    assert.deepEqual(actual, queue.getStats());
    const outcomes = sql(context, 'SELECT outcome, count(*) AS count FROM job_attempts GROUP BY outcome ORDER BY outcome');
    for (const outcome of ['completed', 'failed', 'recovered']) assert.equal(actual.outcomes[outcome], outcomes.find((row) => row.outcome === outcome)?.count ?? 0);
    const [history] = sql(context, `SELECT coalesce(sum(is_retry), 0) AS retries,
      avg(CASE WHEN finished_at IS NOT NULL AND started_at IS NOT NULL THEN max(0, finished_at - started_at) END) AS duration FROM job_attempts`);
    assert.equal(actual.retryAttempts, history.retries); assert.equal(actual.averageDurationMs, history.duration);
    context.evidence.counts = counts; context.evidence.outcomes = outcomes; context.evidence.averageDurationMs = actual.averageDurationMs;
  }],
  '03': ['Persisted job listing and every status filter', async (context) => {
    const { queue } = await fixture(context);
    const all = (await cli(context, ['list'])).data; page(all);
    assert.deepEqual(all.items.map((job) => job.id), sql(context, 'SELECT id FROM jobs ORDER BY seq LIMIT 100').map((row) => row.id));
    context.evidence.filters = {};
    for (const status of [...statuses, 'delayed']) {
      const result = (await cli(context, ['list', '--status', status])).data; page(result);
      result.items.forEach(metadata);
      const expected = status === 'delayed'
        ? sql(context, "SELECT id FROM jobs WHERE status = 'pending' AND run_at > ? ORDER BY seq LIMIT 100", [Date.now()])
        : sql(context, 'SELECT id FROM jobs WHERE status = ? ORDER BY seq LIMIT 100', [status]);
      assert.deepEqual(result.items.map((job) => job.id), expected.map((row) => row.id));
      const sdkPage = queue.listJobs({ status });
      assert.deepEqual(result.items.map((job) => job.id), sdkPage.items.map((job) => job.id));
      assert.equal(result.nextCursor, sdkPage.nextCursor);
      context.evidence.filters[status] = result.items.map((job) => job.id);
    }
    assert.ok(!JSON.stringify(all).includes('fixture-payload-canary'));
  }],
  '04': ['Limits, deterministic insertion order, cursors and boundaries', async (context) => {
    const queue = open(context);
    assert.deepEqual((await cli(context, ['list'])).data, { items: [], nextCursor: null });
    const jobs = Array.from({ length: 1005 }, (_, index) => queue.add('task', { index }, { runAt: 0, priority: index % 5 }));
    const first = (await cli(context, ['list', '--limit', '1'])).data; page(first, 1);
    assert.equal(first.items[0].id, jobs[0].id);
    const defaultPage = (await cli(context, ['list'])).data; page(defaultPage);
    assert.equal(defaultPage.items.length, 100);
    const maximum = (await cli(context, ['list', '--limit', '1000'])).data; page(maximum, 1000);
    assert.equal(maximum.items.length, 1000);
    const tail = (await cli(context, ['list', '--limit', '1000', '--after', String(maximum.nextCursor)])).data; page(tail, 1000);
    assert.equal(tail.items.length, 5); assert.equal(tail.nextCursor, null);
    assert.deepEqual([...maximum.items, ...tail.items].map((job) => job.id), jobs.map((job) => job.id));
    const repeated = (await cli(context, ['list', '--limit', '1'])).data;
    assert.deepEqual(repeated, first);
    assert.deepEqual((await cli(context, ['list', '--after', String(Number.MAX_SAFE_INTEGER)])).data, { items: [], nextCursor: null });
    for (const limit of ['0', '1001', '-1']) await cli(context, ['list', '--limit', limit], { code: 2 });
    context.evidence.pages = { seeded: jobs.length, default: defaultPage.items.length, maximum: maximum.items.length, tail: tail.items.length,
      firstId: first.items[0].id, lastId: tail.items.at(-1).id, cursor: maximum.nextCursor, ordering: 'persistent insertion sequence, independent of priority' };
  }],
  '05': ['Job inspection identifiers, state, timestamps and metadata', async (context) => {
    const queue = open(context); const before = Date.now();
    const job = queue.add('inspect-task', { secret: 'inspect-payload-canary' }, { runAt: 0, priority: 9, attempts: 3,
      backoff: { type: 'exponential', delay: 25, jitter: 0.2 }, idempotencyKey: 'inspect-key-canary' });
    await queue.createWorker().register('inspect-task', () => {}).drain({ timeoutMs: 5000 });
    const result = (await cli(context, ['inspect', job.id])).data;
    keys(result, ['job', 'attempts', 'failures']); metadata(result.job); attempts(result.attempts); attempts(result.failures);
    assert.deepEqual(result.job, queue.getJobSummary(job.id));
    const [persisted] = sql(context, 'SELECT id,name,status,created_at,updated_at,run_at,started_at,finished_at,attempts,max_attempts,priority,lease_expires_at FROM jobs WHERE id = ?', [job.id]);
    for (const [key, column] of Object.entries({ id: 'id', name: 'name', status: 'status', createdAt: 'created_at', updatedAt: 'updated_at',
      runAt: 'run_at', startedAt: 'started_at', finishedAt: 'finished_at', attempts: 'attempts', maxAttempts: 'max_attempts', priority: 'priority', leaseExpiresAt: 'lease_expires_at' })) assert.equal(result.job[key], persisted[column]);
    assert.equal(result.job.status, 'completed'); assert.equal(result.job.attempts, 1);
    assert.ok(result.job.createdAt >= before && result.job.startedAt >= result.job.createdAt && result.job.finishedAt >= result.job.startedAt);
    assert.deepEqual(result.job.backoff, { type: 'exponential', delay: 25, jitter: 0.2 });
    assert.ok(!JSON.stringify(result).includes('canary'));
    context.evidence.inspected = result;
  }],
  '06': ['Failed jobs, persistent attempts and paginated error history', async (context) => {
    const queue = open(context);
    const job = queue.add('fail', {}, { attempts: 3 });
    await queue.createWorker({ pollIntervalMs: 5 }).register('fail', () => {
      throw Object.assign(new Error('Expected temporary failure password=history-secret-canary'), { code: 'E_SIMULATED' });
    }).drain({ timeoutMs: 5000 });
    const inspected = (await cli(context, ['inspect', job.id])).data;
    assert.equal(inspected.job.status, 'failed'); assert.equal(inspected.job.attempts, 3);
    attempts(inspected.attempts); attempts(inspected.failures);
    const rows = sql(context, 'SELECT seq,attempt,outcome,error FROM job_attempts WHERE job_id = ? ORDER BY seq LIMIT 100', [job.id]);
    assert.equal(rows.length, 3);
    assert.deepEqual(inspected.attempts.items.map((attempt) => attempt.id), rows.map((row) => row.seq));
    assert.deepEqual(inspected.failures.items.map((attempt) => attempt.attempt), [1, 2, 3]);
    for (const row of rows) { assert.equal(row.outcome, 'failed'); assert.ok(!row.error.includes('history-secret-canary')); assert.ok(row.error.includes('[REDACTED]')); }
    assert.equal(queue.getFailedJobs().length, 1); assert.equal(queue.getFailureHistory(job.id).items.length, 3);
    const seen = []; let after = 0;
    do {
      const result = (await cli(context, ['inspect', job.id, '--limit', '1', '--after', String(after)])).data;
      attempts(result.failures, 1); seen.push(...result.failures.items.map((attempt) => attempt.id));
      after = result.failures.nextCursor;
    } while (after !== null);
    assert.deepEqual(seen, rows.map((row) => row.seq));
    assert.ok(!JSON.stringify(inspected).includes('history-secret-canary'));
    context.evidence.history = { jobId: job.id, attempts: rows.map(({ seq, attempt, outcome }) => ({ id: seq, attempt, outcome })),
      paginatedIds: seen, persistedCredentialRedacted: true, cliFreeFormErrorsOmitted: true };
  }],
  '07': ['CLI manual retry followed by successful real execution', async (context) => {
    const queue = open(context); const job = queue.add('task', {});
    await queue.createWorker().register('task', () => { throw new Error('Expected failure before manual retry.'); }).drain({ timeoutMs: 5000 });
    const before = queue.getFailureHistory(job.id).items;
    const retried = (await cli(context, ['retry', job.id])).data; metadata(retried);
    assert.equal(retried.id, job.id); assert.equal(retried.status, 'pending'); assert.equal(retried.attempts, 0);
    assert.deepEqual(queue.getFailureHistory(job.id).items, before);
    let executed = 0;
    await queue.createWorker().register('task', () => { executed++; }).drain({ timeoutMs: 5000 });
    const result = (await cli(context, ['inspect', job.id])).data;
    assert.equal(result.job.status, 'completed'); assert.equal(executed, 1); assert.equal(result.job.attempts, 1);
    assert.deepEqual(result.attempts.items.map((attempt) => attempt.outcome), ['failed', 'completed']);
    assert.equal(result.attempts.items[1].retry, true); assert.equal(result.failures.items.length, 1);
    assert.equal(queue.getStats().retryAttempts, 1);
    context.evidence.retriedJob = result; context.evidence.successfulExecutions = executed;
  }],
  '08': ['CLI cancellation of pending and delayed jobs', async (context) => {
    const queue = open(context); const jobs = [queue.add('task', {}), queue.add('task', {}, { delay: 600000 })];
    for (const job of jobs) {
      const result = (await cli(context, ['cancel', job.id])).data; metadata(result);
      assert.equal(result.status, 'cancelled'); assert.equal(result.startedAt, null); assert.equal(result.attempts, 0);
      assert.ok(result.finishedAt >= job.createdAt); assert.equal(queue.getJob(job.id).status, 'cancelled');
      assert.equal(queue.getAttempts(job.id).items.length, 0);
      await cli(context, ['cancel', job.id], { code: 1 });
    }
    let executed = 0;
    await queue.createWorker().register('task', () => { executed++; }).drain({ timeoutMs: 5000 });
    assert.equal(executed, 0);
    context.evidence.cancelled = sql(context, 'SELECT id,status,attempts,started_at,finished_at FROM jobs ORDER BY seq LIMIT 100');
    context.evidence.handlerExecutions = executed;
  }],
  '09': ['Active-job cancellation respects the pending-only contract', async (context) => {
    const queue = open(context); const active = await hold(context, queue);
    const before = queue.getJobSummary(active.job.id);
    const rejected = await cli(context, ['cancel', active.job.id], { code: 1 });
    assert.match(rejected.data.error, /Only pending jobs may be cancelled/);
    assert.equal(rejected.data.retryable, false); assert.equal(active.signal.aborted, false);
    const current = queue.getJobSummary(active.job.id);
    for (const key of ['id', 'status', 'attempts', 'startedAt', 'finishedAt']) assert.equal(current[key], before[key]);
    const stopping = active.worker.stop(); active.release(); await stopping; await active.running;
    const inspected = (await cli(context, ['inspect', active.job.id])).data;
    assert.equal(inspected.job.status, 'completed'); assert.equal(inspected.job.attempts, 1);
    await cli(context, ['cancel', active.job.id], { code: 1 });
    context.evidence.contract = { rejectedWhileActive: true, signalAbortedByCancel: false, finalStatus: inspected.job.status, attempts: inspected.job.attempts };
  }],
  '10': ['Worker claim metadata, exclusive ownership and token privacy', async (context) => {
    const queue = open(context); const active = await hold(context, queue);
    const [row] = sql(context, 'SELECT id,status,attempts,started_at,lease_token,lease_expires_at FROM jobs WHERE id = ?', [active.job.id]);
    assert.equal(typeof row.lease_token, 'string'); assert.ok(row.lease_token.length > 0);
    const claims = queue.getActiveClaims({ limit: 1 }); page(claims, 1);
    assert.equal(claims.items[0].id, row.id); assert.equal(claims.items[0].attempts, row.attempts);
    assert.equal(claims.items[0].startedAt, row.started_at); assert.ok(claims.items[0].leaseExpiresAt >= row.lease_expires_at);
    const listed = (await cli(context, ['list', '--status', 'active'])).data;
    const inspected = (await cli(context, ['inspect', row.id])).data;
    assert.equal(listed.items.length, 1); assert.equal(listed.items[0].id, row.id);
    assert.equal(inspected.job.status, 'active'); assert.equal(inspected.attempts.items[0].outcome, 'active');
    assert.ok(inspected.job.leaseExpiresAt > Date.now());
    assert.ok(!JSON.stringify({ claims, listed, inspected }).includes(row.lease_token));
    // The Stage 2 runner also uses Storage directly to test fencing, never to fake a successful CLI operation.
    const other = new Storage(context.database); context.stores.push(other);
    assert.equal(other.claim(), undefined);
    assert.equal(other.renew(row.id, 'not-the-owner', 30000), false);
    assert.equal(other.finish(row.id, 'not-the-owner', null), false);
    assert.equal(other.finish(row.id, 'not-the-owner', { attempt: 1, at: Date.now(), kind: 'handler', name: 'Error', message: 'Wrong owner' }), false);
    assert.equal(queue.getJobSummary(row.id).status, 'active'); assert.equal(queue.getAttempts(row.id).items.length, 1);
    context.evidence.claim = { jobId: row.id, status: row.status, attempt: row.attempts, startedAt: row.started_at,
      leaseExpiresAt: inspected.job.leaseExpiresAt, ownershipTokenPresentInStorage: true, tokenExposed: false, rejectedNonOwnerWrites: 3 };
  }],
  '11': ['Healthy SQLite diagnostics without data modification', async (context) => {
    const queue = open(context); queue.add('task', {});
    await queue.createWorker().register('task', () => {}).drain({ timeoutMs: 5000 });
    queue.add('task', {}, { delay: 600000 }); await queue.close();
    const before = fingerprint(context);
    const result = (await cli(context, ['doctor'])).data; diagnostics(result);
    assert.equal(result.ok, true); assert.equal(result.schemaVersion, sql(context, 'PRAGMA user_version')[0].user_version);
    for (const name of ['configuration', 'writable-storage', 'integrity', 'schema', 'state', 'leases']) {
      assert.equal(result.checks.find((check) => check.name === name)?.status, 'ok', `Missing or unhealthy diagnostic: ${name}.`);
    }
    assert.deepEqual(result, diagnose({ database: context.database }));
    assert.match((await cli(context, ['doctor'], { json: false })).stdout, /ok: true/);
    assert.equal(fingerprint(context), before);
    context.evidence.diagnostics = result; context.evidence.unchangedSha256 = before;
  }],
  '12': ['Missing, invalid, incompatible and corrupted storage diagnostics', async (context) => {
    const files = {};
    files.missing = join(context.directory, 'missing.db');
    files.invalidParent = join(context.directory, 'missing-parent', 'invalid.db');
    files.directory = join(context.directory, 'directory.db'); mkdirSync(files.directory);
    files.invalidFile = join(context.directory, 'invalid.db'); writeFileSync(files.invalidFile, 'Not a SQLite database.');
    for (const [label, version] of [['incompatible', 99], ['uninitialized', 0], ['malformedSchema', 3], ['corrupted', 0]]) {
      const database = owned(context, join(context.directory, `${label}.db`)); files[label] = database;
      const db = new Database(database);
      try { db.exec("CREATE TABLE sentinel(value TEXT); INSERT INTO sentinel VALUES ('preserve-fixture');"); db.pragma(`user_version = ${version}`); }
      finally { db.close(); }
      if (label === 'corrupted') { const contents = readFileSync(database); contents.fill(0x5a, 0, 32); writeFileSync(database, contents); }
    }
    const legacy = join(context.directory, 'legacy.db'); files.upgradeRequired = legacy;
    await open(context, legacy).close();
    const old = new Database(legacy);
    try { old.exec('DROP TABLE job_attempts; DROP INDEX jobs_status_seq; DROP INDEX jobs_schedule; PRAGMA user_version = 2;'); }
    finally { old.close(); }
    context.evidence.diagnostics = {};
    for (const [label, database] of Object.entries(files)) {
      const regularFile = !['missing', 'invalidParent', 'directory'].includes(label);
      const before = regularFile ? fingerprint(context, database) : null;
      const result = (await cli(context, ['doctor'], { database, code: 1 })).data; diagnostics(result);
      assert.equal(result.ok, false); assert.ok(result.checks.some((check) => check.status === 'error'));
      if (regularFile) assert.equal(fingerprint(context, database), before, `Doctor modified ${label} storage.`);
      if (label === 'missing' || label === 'invalidParent') assert.equal(existsSync(database), false);
      if (label === 'incompatible') assert.deepEqual(sql(context, 'SELECT value FROM sentinel', [], database), [{ value: 'preserve-fixture' }]);
      if (label === 'upgradeRequired') { assert.equal(result.schemaVersion, 2); assert.match(JSON.stringify(result.checks), /Back up storage/); }
      context.evidence.diagnostics[label] = { ...result, unchangedSha256: before };
    }
    const inconsistent = open(context); const job = inconsistent.add('task', {});
    const db = new Database(context.database);
    try { db.prepare('UPDATE jobs SET attempts = max_attempts WHERE id = ?').run(job.id); } finally { db.close(); }
    await inconsistent.close();
    const before = fingerprint(context);
    const result = (await cli(context, ['doctor'], { code: 1 })).data;
    assert.equal(result.checks.find((check) => check.name === 'state')?.status, 'error');
    assert.equal(fingerprint(context), before); context.evidence.diagnostics.inconsistentState = result;
  }],
  '13': ['Explicit --db selection and complete database isolation', async (context) => {
    const defaultDb = join(context.directory, 'queuelite.db'); const selectedDb = join(context.directory, 'selected queue with spaces.db');
    const first = open(context, defaultDb); const firstJob = first.add('default-task', {}); await first.close();
    const second = open(context, selectedDb); const failed = second.add('fail', {});
    await second.createWorker().register('fail', () => { throw new Error('Selected database fixture.'); }).drain({ timeoutMs: 5000 });
    const pending = second.add('task', {}); second.add('task', {}, { delay: 600000 }); await second.close();
    const before = fingerprint(context, defaultDb);
    assert.equal((await cli(context, ['stats'], { database: null })).data.total, 1);
    assert.equal((await cli(context, ['stats'], { database: selectedDb })).data.total, 3);
    await cli(context, ['inspect', firstJob.id], { database: selectedDb, code: 3 });
    await cli(context, ['inspect', failed.id], { database: null, code: 3 });
    assert.equal((await cli(context, ['retry', failed.id], { database: selectedDb })).data.status, 'pending');
    assert.equal((await cli(context, ['cancel', pending.id], { database: selectedDb })).data.status, 'cancelled');
    assert.equal(fingerprint(context, defaultDb), before);
    assert.deepEqual(sql(context, 'SELECT id,status FROM jobs LIMIT 100', [], defaultDb), [{ id: firstJob.id, status: 'pending' }]);
    assert.equal((await cli(context, ['stats'], { database: null })).data.total, 1);
    context.evidence.isolation = { defaultDb, selectedDb, defaultUnchangedSha256: before,
      selectedJobs: sql(context, 'SELECT id,status FROM jobs ORDER BY seq LIMIT 100', [], selectedDb) };
  }],
  '14': ['JSON schemas and human-readable output for every CLI command', async (context) => {
    const queue = open(context); const failed = [queue.add('fail', {}), queue.add('fail', {})];
    const done = queue.add('done', {});
    await queue.createWorker().register('fail', () => { throw new Error('Format fixture.'); }).register('done', () => {}).drain({ timeoutMs: 5000 });
    const pending = [queue.add('task', {}), queue.add('task', {})];
    const checks = [
      ['stats', [], stats, /total:/], ['list', [], (value) => { page(value); value.items.forEach(metadata); }, /ID\s+STATUS/],
      ['inspect', [done.id], (value) => { keys(value, ['job', 'attempts', 'failures']); metadata(value.job); attempts(value.attempts); attempts(value.failures); }, /job:/],
      ['doctor', [], diagnostics, /checks:/],
    ];
    for (const [command, args, validate, humanPattern] of checks) {
      validate((await cli(context, [command, ...args])).data);
      assert.match((await cli(context, [command, ...args], { json: false })).stdout, humanPattern);
    }
    for (const [command, jobs, status] of [['retry', failed, 'pending'], ['cancel', pending, 'cancelled']]) {
      const result = (await cli(context, [command, jobs[0].id])).data; metadata(result); assert.equal(result.status, status);
      assert.match((await cli(context, [command, jobs[1].id], { json: false })).stdout, new RegExp(`status: "${status}"`));
    }
    keys((await cli(context, ['--help'], { database: null })).data, ['help']);
    const special = queue.add('terminal\u001b[31m\nnext', {});
    const terminal = await cli(context, ['list'], { json: false });
    assert.ok(terminal.stdout.includes(special.id)); assert.ok(!terminal.stdout.includes('\u001b'));
    assert.ok(terminal.stdout.includes('\\u001b') && terminal.stdout.includes('\\nnext'));
    context.evidence.formats = { jsonCommands: ['stats', 'list', 'inspect', 'doctor', 'retry', 'cancel', 'help'],
      humanCommands: ['stats', 'list', 'inspect', 'doctor', 'retry', 'cancel'], escapedTerminalControls: true };
  }],
  '15': ['Invalid arguments, unknown jobs, errors and process exit codes', async (context) => {
    const queue = open(context); const completed = queue.add('done', {});
    await queue.createWorker().register('done', () => {}).drain({ timeoutMs: 5000 });
    const invalid = [ ['unknown'], ['retry'], ['cancel'], ['inspect'], ['stats', 'extra'], ['retry', 'a', 'b'], ['--unknown'],
      ['list', '--limit', '0'], ['list', '--limit', '1001'], ['list', '--limit', '1.5'], ['list', '--limit', 'NaN'],
      ['list', '--after', '-1'], ['list', '--after', '9007199254740992'], ['list', '--status', 'deleted'],
      ['stats', '--status', 'failed'], ['doctor', '--limit', '1'], ['retry', 'job', '--after', '1'],
      ['stats', '--db'], ['list', '--db', ':memory:'], ['stats', '--db', ''] ];
    for (const args of invalid) {
      const result = await cli(context, args, { database: null, code: 2 });
      keys(result.data, ['error', 'retryable']); assert.ok(result.data.error.trim().length >= 10); assert.equal(result.data.retryable, false);
      assert.ok(!result.stdout.includes('at main ('), 'Error output leaked a stack trace.');
    }
    for (const command of ['inspect', 'retry', 'cancel']) {
      const result = await cli(context, [command, 'missing-job-id'], { code: 3 });
      keys(result.data, ['error', 'retryable']); assert.match(result.data.error, /not found/i);
    }
    for (const command of ['retry', 'cancel']) {
      const result = await cli(context, [command, completed.id], { code: 1 });
      assert.match(result.data.error, /Only (failed|pending) jobs/);
    }
    const missing = join(context.directory, 'no-database.db');
    for (const command of ['stats', 'list', 'inspect', 'retry', 'cancel']) {
      const result = await cli(context, [command, ...(['inspect', 'retry', 'cancel'].includes(command) ? ['job'] : [])], { database: missing, code: 1 });
      assert.match(result.data.error, /path|permissions|doctor/i); assert.equal(existsSync(missing), false);
    }
    const human = await cli(context, ['list', '--limit', '0'], { code: 2, json: false });
    assert.match(human.stdout, /between 1 and 1000/);
    context.evidence.exitCodes = { invalidUsage: 2, missingJob: 3, invalidStateOrStorage: 1, validCommands: 0, invalidArgumentCases: invalid.length };
  }],
  '16': ['Lifecycle ordering, logging, recovery, redaction and observer failures', async (context) => {
    const events = []; const logs = []; const snapshots = [];
    const queue = open(context, context.database, { onEvent: (event) => {
      events.push(event); snapshots.push(queue.getJobSummary(event.jobId));
    }, logger: { info: (event) => logs.push(event) } });
    const job = queue.add('name-private-canary', { token: 'payload-private-canary' }, { attempts: 2, idempotencyKey: 'key-private-canary' });
    queue.add('name-private-canary', {}, { idempotencyKey: 'key-private-canary' });
    let calls = 0;
    await queue.createWorker({ pollIntervalMs: 5 }).register('name-private-canary', () => {
      if (++calls === 1) throw new Error('password=credential-private-canary Bearer bearer-private-canary');
    }).drain({ timeoutMs: 5000 });
    assert.deepEqual(events.map((event) => event.type), ['enqueued', 'started', 'failed', 'retried', 'started', 'completed']);
    assert.deepEqual(events, logs); assert.ok(snapshots.every((snapshot) => snapshot?.id === job.id));
    assert.ok(events.every(Object.isFrozen)); assert.ok(events.at(-1).durationMs >= 0);
    assert.ok(!JSON.stringify(logs).includes('private-canary'));
    assert.ok(!JSON.stringify(queue.getFailureHistory(job.id)).includes('credential-private-canary'));
    for (const event of events) {
      keys(event, event.type === 'completed' ? ['type', 'jobId', 'at', 'attempt', 'status', 'durationMs'] : ['type', 'jobId', 'at', 'attempt', 'status']);
      assert.ok(Number.isSafeInteger(event.at)); assert.ok(Number.isSafeInteger(event.attempt)); assert.ok(statuses.includes(event.status));
    }
    const delayed = queue.add('task', {}, { delay: 600000 }); queue.cancelJob(delayed.id);
    assert.equal(events.at(-1).type, 'cancelled');
    const active = await hold(context, queue, 'recover', { leaseDurationMs: 200, heartbeatIntervalMs: 50 });
    await assert.rejects(active.worker.stop({ timeoutMs: 0 }), /Shutdown deadline exceeded/); await active.running;
    assert.equal(active.signal.aborted, true);
    const expiration = queue.getJobSummary(active.job.id).leaseExpiresAt;
    await until(context, () => Date.now() > expiration + 5, 'Abandoned verification lease did not expire.');
    const diagnostic = (await cli(context, ['doctor'], { code: 1 })).data;
    assert.equal(diagnostic.checks.find((check) => check.name === 'leases')?.status, 'warning');
    assert.equal(queue.getJobSummary(active.job.id).status, 'active', 'Doctor silently recovered the claim.');
    await queue.createWorker({ pollIntervalMs: 5 }).register('recover', () => {}).drain({ timeoutMs: 5000 });
    const recovered = queue.getJob(active.job.id); active.release(); await sleep(20);
    assert.deepEqual(queue.getJob(active.job.id), recovered, 'The late abandoned handler overwrote its replacement.');
    assert.equal(recovered.status, 'completed'); assert.equal(recovered.attempts, 2);
    assert.deepEqual(events.filter((event) => event.jobId === active.job.id).map((event) => event.type), ['enqueued', 'started', 'failed', 'recovered', 'retried', 'started', 'completed']);
    const observerDb = join(context.directory, 'observer-errors.db'); let hooks = 0; let loggerCalls = 0;
    const unhandled = []; const listener = (error) => unhandled.push(error); process.on('unhandledRejection', listener);
    try {
      const observer = open(context, observerDb, { onEvent: async () => { hooks++; throw new Error('Expected observer rejection.'); },
        logger: { info: () => { loggerCalls++; throw new Error('Expected logger failure.'); } } });
      const observed = observer.add('task', {}); let executed = 0;
      await observer.createWorker().register('task', () => { executed++; }).drain({ timeoutMs: 5000 }); await sleep(0);
      assert.equal(observer.getJobSummary(observed.id).status, 'completed'); assert.equal(executed, 1);
      assert.equal(hooks, 3); assert.equal(loggerCalls, 3); assert.deepEqual(unhandled, []);
    } finally { process.off('unhandledRejection', listener); }
    const lock = new Database(context.database); lock.exec('BEGIN IMMEDIATE');
    try { assert.throws(() => queue.add('task', {}), (error) => error instanceof QueueLiteError && error.retryable === true && error.code === 'SQLITE_BUSY' && /bounded asynchronous backoff/.test(error.message)); }
    finally { lock.exec('ROLLBACK'); lock.close(); }
    context.evidence.observability = { events, payloadsAndCredentialsOmitted: true, recoveredJobId: recovered.id,
      expiredLeaseDoctor: diagnostic, observerRejectionsHandled: hooks, loggerFailuresIsolated: loggerCalls, unhandledRejections: unhandled.length, transientBusyErrorActionable: true };
  }],
  '17': ['SDK/CLI compatibility, documented examples, packaged imports and Stage 1/2 regressions', async (context) => {
    const queue = open(context); const first = queue.add('task', { saved: true }, { idempotencyKey: 'compatibility-key', runAt: 0 });
    assert.equal(queue.add('task', { saved: false }, { idempotencyKey: 'compatibility-key' }).id, first.id);
    await queue.close();
    const restarted = open(context);
    assert.deepEqual(restarted.getJob(first.id).data, { saved: true });
    await restarted.createWorker({ concurrency: 1 }).register('task', (job) => { assert.equal(job.data.saved, true); }).drain({ timeoutMs: 5000 });
    assert.deepEqual((await cli(context, ['inspect', first.id])).data.job, restarted.getJobSummary(first.id));
    assert.throws(() => restarted.add('bad', { value: undefined }), QueueLiteError);
    const readOnly = open(context, context.database, { readOnly: true }); assert.equal(readOnly.getStats().completed, 1);
    assert.throws(() => readOnly.createWorker(), /read-only/);
    const examples = [];
    for (const name of ['welcome-email', 'account-provisioning', 'retry-recovery', 'delayed-job']) {
      const result = await execute(context, join(root, 'examples', `${name}.mjs`), []);
      assert.equal(result.code, 0, `Documented example failed: ${name}. ${result.stderr}`); assert.match(result.stdout, /PASS/);
      examples.push({ name, exitCode: result.code, output: result.stdout.trim() });
    }
    const regression = await execute(context, join(root, 'scripts', 'verify-stage2.mjs'), [], {
      timeout: 180000, excerpt: 200,
      env: Object.fromEntries(Object.entries(process.env).filter(([name]) => name.toLowerCase() !== 'npm_config_case')),
    });
    assert.equal(regression.code, 0, `Stage 2 operational regressions failed. ${regression.stdout.slice(-5000)} ${regression.stderr}`);
    assert.match(regression.stdout, /PASS TOTAL: 13\/13/);
    const packageResult = await execute(context, join(root, 'scripts', 'verify-package.mjs'), [], { timeout: 120000, excerpt: 1000 });
    assert.equal(packageResult.code, 0, `Packaged public import/CLI/declarations/examples failed; registry access or populated npm cache may be required. ${packageResult.stderr.slice(-5000)}`);
    assert.match(packageResult.stdout, /PASS Package tarball:/);
    context.evidence.compatibility = { persistedJobId: first.id, publicImport: '@thebraz/queuelite', sdkAndCliSnapshotsMatch: true,
      examples, stage2Summary: regression.stdout.trim().split(/\r?\n/).at(-1), packageVerification: packageResult.stdout.trim() };
  }],
};

async function verify(id, directory) {
  const [description, run] = cases[id];
  const caseDirectory = join(directory, `case-${id}`); mkdirSync(caseDirectory);
  const context = { directory: caseDirectory, database: join(caseDirectory, `case-${id}.db`), evidence: {}, queues: [], stores: [], release: [], errors: [] };
  const began = performance.now(); let failure;
  try { await run(context); if (context.errors.length) throw context.errors[0]; }
  catch (error) { failure = error; }
  finally {
    for (const release of context.release) release();
    const closing = await Promise.allSettled(context.queues.map((queue) => queue.close({ timeoutMs: 1000 })));
    for (const result of closing) if (result.status === 'rejected') failure ??= result.reason;
    for (const store of context.stores) { try { store.close(); } catch (error) { failure ??= error; } }
    failure ??= context.errors[0];
  }
  const status = failure ? 'FAIL' : 'PASS';
  console.log(`${status} ${id} - ${description}`);
  console.log(JSON.stringify({ database: context.database, elapsedMs: Math.round(performance.now() - began), ...context.evidence,
    ...(failure ? { error: failure.message, details: failure.stack } : {}) }, null, 2));
  return status;
}

try {
  const { values, positionals } = parseArgs({ options: { case: { type: 'string' } }, strict: true, allowPositionals: true });
  // npm.ps1 may consume --case and pass its value positionally or via npm_config_case.
  const forwarded = process.env.npm_config_case;
  const requested = values.case ?? (forwarded === undefined ? undefined : positionals.length === 1 ? positionals.shift() : forwarded);
  if (positionals.length) throw new Error('Unexpected arguments. Use --case followed by 01 through 17.');
  if (requested !== undefined && !Object.hasOwn(cases, requested)) throw new Error('Choose a case from 01 through 17.');
  const directory = resolve(mkdtempSync(join(tmpdir(), 'queuelite-stage3-')));
  console.log(`Isolated Stage 3 verification databases: ${directory}`);
  const results = { PASS: 0, FAIL: 0, SKIP: 0 };
  for (const id of Object.keys(cases).sort()) {
    if (requested !== undefined && requested !== id) {
      console.log(`SKIP ${id} - ${cases[id][0]} (not selected by --case ${requested})`); results.SKIP++;
    } else results[await verify(id, directory)]++;
  }
  console.log(`STAGE 3 VERIFICATION\nTotal: 17\nPassed: ${results.PASS}\nFailed: ${results.FAIL}\nSkipped: ${results.SKIP}`);
  console.log('Temporary verification databases retained for inspection. No user database was selected.');
  console.log(results.FAIL ? 'VERDICT: Stage 3 is not ready to advance; resolve the failures reported above.'
    : results.SKIP ? 'VERDICT: Selected checks passed; run all 17 cases before deciding Stage 4 readiness.'
      : 'VERDICT: Stage 3 verification passed on this host; ready for Stage 4 review. Other platforms and real external effects remain unverified.');
  process.exitCode = results.FAIL ? 1 : 0;
} catch (error) {
  console.error(`FAIL - ${error.message}\nUsage: npm run verify:stage3 -- --case 01 (01–17); omit --case to run all cases.`);
  process.exitCode = 2;
}
