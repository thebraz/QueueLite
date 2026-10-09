import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { performance } from 'node:perf_hooks';
import { setTimeout as sleep } from 'node:timers/promises';
import { parseArgs } from 'node:util';
import Database from 'better-sqlite3';
import { createQueue } from '../dist/index.js';
import { Storage } from '../dist/storage.js';

async function until(context, predicate, explanation, timeout = 15000) {
  const deadline = performance.now() + timeout;
  while (true) {
    if (context.errors.length) throw context.errors[0];
    const value = predicate();
    if (value) return value;
    if (performance.now() >= deadline) throw new Error(explanation);
    await sleep(10);
  }
}
function open(context) {
  const queue = createQueue({ database: context.database });
  context.queues.push(queue);
  return queue;
}
function add(context, queue, data = {}, options = {}) {
  const job = queue.add('task', data, options);
  context.evidence.jobIds ??= [];
  context.evidence.jobIds.push(job.id);
  return job;
}
function start(context, worker, options) {
  const running = worker.start(options);
  void running.catch((error) => context.errors.push(error));
  return running;
}
async function terminal(context, queue, ids, timeout = 15000, observe = () => {}) {
  return until(context, () => {
    const jobs = ids.map((id) => queue.getJob(id));
    context.evidence.jobs = jobs.map((job) => job && ({ id: job.id, status: job.status, attempts: job.attempts, error: job.error }));
    jobs.forEach(observe);
    return jobs.every((job) => job && ['completed', 'failed'].includes(job.status)) && jobs;
  }, 'Jobs did not reach a final state before the verification deadline.', timeout);
}
function completed(jobs) {
  for (const job of jobs) assert.equal(job.status, 'completed', `Job ${job.id} did not succeed: ${job.error ?? job.status}`);
}
function child(context, config) {
  const process = fork(new URL('./fixtures/stage2-child.mjs', import.meta.url), [JSON.stringify({ ...config, database: context.database })], {
    stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
  });
  const state = { process, messages: [], closed: false, expectedKill: false, code: null, signal: null, stderr: '' };
  context.children.push(state);
  process.stdout.on('data', () => {});
  process.stderr.on('data', (chunk) => { state.stderr = (state.stderr + chunk.toString()).slice(-4000); });
  process.on('message', (message) => {
    state.messages.push(message);
    if (message.type === 'error') context.errors.push(new Error(`Worker process ${process.pid}: ${message.message}`));
  });
  process.on('error', (error) => context.errors.push(error));
  process.on('close', (code, signal) => {
    state.closed = true; state.code = code; state.signal = signal;
    if (!state.expectedKill && code !== 0) context.errors.push(new Error(`Worker process ${process.pid} exited unexpectedly (${code ?? signal}): ${state.stderr}`));
  });
  state.receive = (type) => until(context, () => {
    const index = state.messages.findIndex((message) => message.type === type);
    if (index >= 0) return state.messages.splice(index, 1)[0];
    if (state.closed) throw new Error(`Worker process ${process.pid} exited without reporting ${type}. ${state.stderr}`);
    return false;
  }, `Worker process ${process.pid} did not report ${type}.`);
  state.go = () => process.send('go');
  state.finish = async () => {
    await until(context, () => state.closed, `Worker process ${process.pid} did not exit.`, 5000);
    assert.equal(state.code, 0, `Worker process failed: ${state.stderr}`);
  };
  state.kill = async () => {
    state.expectedKill = true;
    assert.ok(process.kill('SIGKILL'), 'Could not terminate the verification worker.');
    await until(context, () => state.closed, 'The terminated worker did not exit.', 5000);
  };
  return state;
}
async function childResult(context, config) {
  const process = child(context, config);
  await process.receive('ready'); process.go();
  const result = await process.receive('done'); await process.finish();
  return { ...result, pid: process.process.pid };
}

const cases = {
  '01': ['Concurrency 2: six real two-second jobs', async (context) => {
    const queue = open(context);
    const jobs = Array.from({ length: 6 }, () => add(context, queue));
    const executions = []; let active = 0; let maximum = 0;
    const worker = queue.createWorker({ pollIntervalMs: 10 }).register('task', async (job) => {
      const execution = { id: job.id, startedAt: Date.now(), active: ++active };
      const began = performance.now(); maximum = Math.max(maximum, active); executions.push(execution);
      try {
        // Timers can wake slightly early; keep the measured workload at least two seconds.
        while (performance.now() - began < 2000) await sleep(Math.max(1, Math.ceil(2000 - (performance.now() - began))));
      } finally { execution.durationMs = Math.round(performance.now() - began); active--; }
    });
    context.evidence.executions = executions;
    const began = performance.now(); const running = start(context, worker, { concurrency: 2 });
    completed(await terminal(context, queue, jobs.map((job) => job.id), 20000));
    await worker.stop(); await running;
    context.evidence.processingMs = Math.round(performance.now() - began); context.evidence.maximumConcurrency = maximum;
    assert.equal(maximum, 2, 'The worker did not respect concurrency 2.');
    assert.equal(executions.length, 6, 'All six jobs must execute exactly once.');
    assert.ok(executions.every((execution) => execution.durationMs >= 2000), 'Each handler must run for at least two seconds.');
    assert.ok(context.evidence.processingMs >= 6000, 'Six two-second jobs at concurrency 2 cannot finish before six seconds.');
    assert.ok(jobs.every((job) => queue.getJob(job.id).attempts === 1), 'An unexpected duplicate claim occurred.');
  }],
  '02': ['Two worker processes: ten atomic claims', async (context) => {
    const queue = open(context); const jobs = Array.from({ length: 10 }, () => add(context, queue));
    const ids = jobs.map((job) => job.id);
    const workers = [child(context, { mode: 'worker', ids, durationMs: 250 }), child(context, { mode: 'worker', ids, durationMs: 250 })];
    await Promise.all(workers.map((worker) => worker.receive('ready')));
    workers.forEach((worker) => worker.go());
    const results = await Promise.all(workers.map((worker) => worker.receive('done')));
    await Promise.all(workers.map((worker) => worker.finish()));
    const executions = results.flatMap((result) => result.executions);
    context.evidence.workers = workers.map((worker, i) => ({ pid: worker.process.pid, executions: results[i].executions.length }));
    context.evidence.executions = executions;
    assert.ok(results.every((result) => result.executions.length > 0), 'Both worker processes must actually claim jobs.');
    assert.equal(executions.length, 10, 'Expected ten executions, with no duplicate claims.');
    assert.deepEqual(executions.map((execution) => execution.id).sort(), [...ids].sort(), 'The workers lost or duplicated jobs.');
    const snapshots = await terminal(context, queue, ids); completed(snapshots);
    assert.ok(snapshots.every((job) => job.attempts === 1), 'A job was claimed more than once.');
  }],
  '03': ['Fixed retry: one-second intervals, two failures then success', async (context) => {
    const queue = open(context); const job = add(context, queue, {}, { attempts: 3, backoff: { type: 'fixed', delay: 1000 } });
    const executions = []; const retryPlans = new Map();
    const worker = queue.createWorker({ pollIntervalMs: 10 }).register('task', (snapshot) => {
      executions.push({ attempt: snapshot.attempts, at: Date.now() });
      if (snapshot.attempts <= 2) throw new Error(`Expected fixed retry failure ${snapshot.attempts}`);
    });
    const running = start(context, worker);
    const [result] = await terminal(context, queue, [job.id], 10000, (snapshot) => {
      if (snapshot.status === 'pending' && snapshot.attempts > 0) retryPlans.set(snapshot.attempts, snapshot.runAt - snapshot.updatedAt);
    });
    await worker.stop(); await running;
    context.evidence.executions = executions; context.evidence.retryDelaysMs = [...retryPlans.values()]; context.evidence.errors = result.errorHistory;
    completed([result]); assert.equal(result.attempts, 3, 'Expected exactly three attempts.');
    assert.equal(result.errorHistory.length, 2, 'The two failures were not retained.');
    assert.deepEqual([...retryPlans.values()], [1000, 1000], 'Fixed retries must be scheduled one second apart.');
    for (let i = 1; i < executions.length; i++) assert.ok(executions[i].at - result.errorHistory[i - 1].at >= 1000, 'A fixed retry executed too early.');
  }],
  '04': ['Exponential backoff: one-second base, without and with bounded jitter', async (context) => {
    const queue = open(context); context.evidence.variants = [];
    for (const jitter of [0, 0.25]) {
      const job = add(context, queue, {}, { attempts: 4, backoff: { type: 'exponential', delay: 1000, jitter } });
      const plans = new Map(); const executions = [];
      const worker = queue.createWorker({ pollIntervalMs: 10 }).register('task', (snapshot) => {
        executions.push({ attempt: snapshot.attempts, at: Date.now() });
        if (snapshot.attempts < 4) throw new Error(`Expected exponential retry failure ${snapshot.attempts}`);
      });
      const running = start(context, worker);
      const [result] = await terminal(context, queue, [job.id], 15000, (snapshot) => {
        if (snapshot.status === 'pending' && snapshot.attempts > 0) plans.set(snapshot.attempts, { delayMs: snapshot.runAt - snapshot.updatedAt, runAt: snapshot.runAt });
      });
      await worker.stop(); await running;
      context.evidence.variants.push({ id: job.id, jitter, executions, retryPlans: [...plans.values()], errors: result.errorHistory });
      completed([result]); assert.equal(result.attempts, 4, 'Exponential retry must succeed on attempt four.');
      assert.equal(result.errorHistory.length, 3, 'All three exponential failures must be recorded.');
      assert.equal(plans.size, 3, 'Did not observe all three persisted retry timestamps.');
      for (let attempt = 1; attempt <= 3; attempt++) {
        const base = 1000 * 2 ** (attempt - 1); const plan = plans.get(attempt);
        assert.ok(plan.delayMs >= Math.floor(base * (1 - jitter)) && plan.delayMs <= base, `Retry ${attempt} is outside its configured jitter bounds.`);
        if (jitter === 0) assert.equal(plan.delayMs, base, 'Exponential delays must be 1000, 2000 and 4000 milliseconds.');
        assert.ok(executions[attempt].at >= plan.runAt, 'An exponential retry executed before its persisted UTC timestamp.');
      }
    }
  }],
  '05': ['Retry exhaustion respects the total attempt limit', async (context) => {
    const queue = open(context); const job = add(context, queue, {}, { attempts: 3, backoff: { type: 'fixed', delay: 100 } }); let count = 0;
    const worker = queue.createWorker({ pollIntervalMs: 10 }).register('task', () => { count++; throw new Error('Expected permanent verification failure'); });
    const running = start(context, worker); const [result] = await terminal(context, queue, [job.id]);
    await sleep(350); await worker.stop(); await running;
    context.evidence.executionCount = count; context.evidence.errors = result.errorHistory;
    assert.equal(result.status, 'failed', 'An exhausted job must be terminally failed.');
    assert.equal(result.attempts, 3); assert.equal(count, 3, 'The handler ran outside its configured attempt budget.');
    assert.equal(result.errorHistory.length, 3); assert.deepEqual(queue.getJob(job.id), result, 'A terminal failure changed after its retry budget was exhausted.');
  }],
  '06': ['Delayed execution: ten seconds in the future, UTC', async (context) => {
    const queue = open(context); const runAt = Date.now() + 10000;
    const job = add(context, queue, {}, { runAt }); let executionCount = 0;
    const worker = queue.createWorker({ pollIntervalMs: 10 }).register('task', () => { executionCount++; });
    const running = start(context, worker); let pendingObservations = 0;
    const [result] = await terminal(context, queue, [job.id], 20000, (snapshot) => {
      if (Date.now() < runAt) {
        pendingObservations++;
        assert.equal(snapshot.status, 'pending', 'The delayed job was claimed before its UTC timestamp.');
        assert.equal(executionCount, 0, 'The delayed handler ran too early.');
      }
    });
    await worker.stop(); await running;
    context.evidence.scheduledUtc = new Date(runAt).toISOString(); context.evidence.startedUtc = new Date(result.startedAt).toISOString();
    context.evidence.latenessMs = result.startedAt - runAt; context.evidence.executionCount = executionCount; context.evidence.pendingObservations = pendingObservations;
    completed([result]); assert.equal(result.runAt, runAt); assert.ok(result.startedAt >= runAt, 'The job started before the requested UTC time.');
    assert.ok(pendingObservations > 0, 'The runner never observed the job waiting.'); assert.equal(executionCount, 1);
  }],
  '07': ['Priority ordering and FIFO ties with one worker', async (context) => {
    const queue = open(context); const priorities = [0, 5, 5, -1, 10];
    const jobs = priorities.map((priority, index) => add(context, queue, { index }, { priority }));
    const delayed = add(context, queue, { index: 99 }, { priority: 1000, delay: 60000 }); const order = [];
    const worker = queue.createWorker({ concurrency: 1, pollIntervalMs: 10 }).register('task', (job) => { order.push(job.data.index); });
    await worker.drain({ timeoutMs: 5000 });
    context.evidence.observedOrder = order; context.evidence.expectedOrder = [4, 1, 2, 0, 3]; context.evidence.executionCount = order.length;
    assert.deepEqual(order, [4, 1, 2, 0, 3], 'Eligible jobs must run by descending priority, then FIFO.');
    completed(jobs.map((job) => queue.getJob(job.id))); assert.equal(queue.getJob(delayed.id).status, 'pending', 'A high-priority delayed job ran prematurely.');
  }],
  '08': ['Idempotency deduplicates sequential and concurrent submissions', async (context) => {
    const queue = open(context);
    const first = queue.add('task', { original: true }, { idempotencyKey: 'sequential-key' });
    const duplicate = queue.add('task', { original: false }, { idempotencyKey: 'sequential-key' });
    assert.equal(duplicate.id, first.id, 'A repeated key created a new job.'); assert.deepEqual(duplicate.data, first.data, 'A duplicate changed the original payload.');
    const producers = [child(context, { mode: 'produce' }), child(context, { mode: 'produce' })];
    await Promise.all(producers.map((producer) => producer.receive('ready'))); producers.forEach((producer) => producer.go());
    const results = await Promise.all(producers.map((producer) => producer.receive('done'))); await Promise.all(producers.map((producer) => producer.finish()));
    const ids = results.flatMap((result) => result.ids);
    const db = new Database(context.database, { readonly: true, fileMustExist: true });
    let count;
    try { count = db.prepare("SELECT count(*) AS count FROM jobs WHERE name = 'task' AND idempotency_key = ?").get('stage2-concurrent-key').count; }
    finally { db.close(); }
    context.evidence.jobIds = [first.id, ...new Set(ids)]; context.evidence.submissionCount = ids.length + 2;
    context.evidence.concurrentRows = count; context.evidence.producers = producers.map((producer, i) => ({ pid: producer.process.pid, submissions: results[i].ids.length, busyRetries: results[i].busyRetries }));
    assert.equal(ids.length, 40); assert.equal(new Set(ids).size, 1, 'Concurrent producers inserted duplicate jobs.'); assert.equal(count, 1, 'SQLite contains duplicate effective keys.');
    const worker = queue.createWorker({ pollIntervalMs: 10 }).register('task', () => {}); await worker.drain({ timeoutMs: 5000 });
    assert.equal(queue.add('task', {}, { idempotencyKey: 'sequential-key' }).id, first.id, 'A completed job lost its idempotency key.');
  }],
  '09': ['Abrupt process crash, expired lease and recovery', async (context) => {
    const queue = open(context); const job = add(context, queue, {}, { attempts: 3 });
    const crashed = child(context, { mode: 'crash', ids: [job.id], leaseDurationMs: 1000 });
    await crashed.receive('ready'); crashed.go(); const firstClaim = await crashed.receive('claimed'); await crashed.kill();
    const abandoned = queue.getJob(job.id); assert.equal(abandoned.status, 'active'); assert.equal(abandoned.attempts, 1);
    const result = await childResult(context, { mode: 'worker', ids: [job.id] });
    const recovered = queue.getJob(job.id);
    context.evidence.crashedPid = crashed.process.pid; context.evidence.crashExit = { code: crashed.code, signal: crashed.signal };
    context.evidence.recoveryPid = result.pid; context.evidence.executions = [firstClaim, ...result.executions]; context.evidence.expiredLeaseUtc = new Date(abandoned.leaseExpiresAt).toISOString(); context.evidence.errors = recovered.errorHistory;
    completed([recovered]); assert.equal(recovered.attempts, 2); assert.equal(result.executions.length, 1);
    assert.ok(recovered.startedAt >= abandoned.leaseExpiresAt, 'The recovery worker reclaimed a still-valid lease.');
    assert.equal(recovered.errorHistory.length, 1); assert.equal(recovered.errorHistory[0].kind, 'lease-expired', 'Recovery did not record the abandoned claim.');
  }],
  '10': ['Heartbeat renewal, ownership and stale-token rejection', async (context) => {
    const queue = open(context); const job = add(context, queue, {}, { attempts: 3 });
    const storage = new Storage(context.database); context.stores.push(storage);
    let release; let signal;
    const gate = new Promise((resolve) => { release = resolve; }); context.release.push(release);
    const worker = queue.createWorker({ pollIntervalMs: 10, leaseDurationMs: 600, heartbeatIntervalMs: 100 }).register('task', async (_, execution) => { signal = execution.signal; await gate; });
    const running = start(context, worker); await until(context, () => signal, 'The handler did not start.');
    const first = queue.getJob(job.id);
    const db = new Database(context.database, { readonly: true, fileMustExist: true });
    let oldToken;
    try { oldToken = db.prepare('SELECT lease_token FROM jobs WHERE id = ?').get(job.id).lease_token; }
    finally { db.close(); }
    assert.equal(typeof oldToken, 'string', 'The active claim has no lease token.');
    await sleep(1300); const renewed = queue.getJob(job.id);
    context.evidence.initialLeaseExpiresAt = first.leaseExpiresAt; context.evidence.renewedLeaseExpiresAt = renewed.leaseExpiresAt;
    assert.equal(renewed.status, 'active'); assert.equal(renewed.attempts, 1);
    assert.ok(renewed.leaseExpiresAt > first.leaseExpiresAt && renewed.leaseExpiresAt > Date.now(), 'Heartbeat did not maintain a live lease.');
    assert.equal(storage.claim(), undefined, 'Another owner claimed a job while its lease remained valid.');
    assert.equal(storage.finish(job.id, 'wrong-token', null), false, 'A non-owner completed the live job.');
    assert.equal(storage.renew(job.id, 'wrong-token', 600), false, 'A non-owner renewed the live job.');
    await assert.rejects(worker.stop({ timeoutMs: 0 }), /Shutdown deadline exceeded/); await running;
    assert.equal(signal.aborted, true, 'Abandonment did not cancel the old handler cooperatively.');
    const expiration = queue.getJob(job.id).leaseExpiresAt;
    await until(context, () => Date.now() >= expiration + 10, 'The abandoned lease did not expire.');
    const replacement = storage.claim(5000); assert.ok(replacement, 'The expired job was not recovered.');
    assert.equal(replacement.job.id, job.id); assert.notEqual(replacement.token, oldToken);
    assert.equal(replacement.job.attempts, 2); assert.equal(replacement.job.errorHistory[0]?.kind, 'lease-expired');
    const before = queue.getJob(job.id);
    assert.equal(storage.finish(job.id, oldToken, null), false, 'The stale owner completed its replacement claim.');
    assert.equal(storage.finish(job.id, oldToken, { attempt: 1, at: Date.now(), kind: 'handler', name: 'Error', message: 'Stale verification failure' }), false, 'The stale owner failed its replacement claim.');
    assert.equal(storage.renew(job.id, oldToken, 5000), false, 'The stale owner renewed its replacement claim.');
    assert.deepEqual(queue.getJob(job.id), before, 'Rejected stale operations changed persisted job state.');
    assert.equal(storage.finish(job.id, replacement.token, null), true, 'The real replacement owner could not complete.');
    const final = queue.getJob(job.id); release(); await sleep(30);
    assert.deepEqual(queue.getJob(job.id), final, 'The late original handler overwrote the completed replacement.');
    context.evidence.attempts = final.attempts; context.evidence.staleOperationsRejected = 3; context.evidence.nonOwnerOperationsRejected = 2;
    context.evidence.oldHandlerAborted = signal.aborted; context.evidence.errors = final.errorHistory;
  }],
  '11': ['Failure inspection and explicit manual retry', async (context) => {
    const queue = open(context); const job = add(context, queue); let fail = true; let count = 0;
    const worker = queue.createWorker({ pollIntervalMs: 10 }).register('task', () => { count++; if (fail) throw new Error('Expected inspectable failure'); });
    await worker.drain({ timeoutMs: 5000 }); const failures = queue.getFailedJobs();
    assert.equal(failures.length, 1); assert.equal(failures[0].id, job.id); assert.match(failures[0].error, /Expected inspectable failure/);
    fail = false; const retried = queue.retryJob(job.id);
    assert.equal(retried.status, 'pending'); assert.equal(retried.attempts, 0); assert.equal(retried.errorHistory.length, 1);
    await worker.drain({ timeoutMs: 5000 }); const result = queue.getJob(job.id);
    completed([result]); assert.equal(result.attempts, 1); assert.equal(result.errorHistory.length, 1); assert.equal(queue.getFailedJobs().length, 0);
    assert.throws(() => queue.retryJob(job.id), /Only failed jobs/, 'A completed job was incorrectly retried.');
    context.evidence.executionCount = count; context.evidence.failedBefore = failures.length; context.evidence.failedAfter = queue.getFailedJobs().length; context.evidence.errors = result.errorHistory;
    assert.equal(count, 2, 'Manual retry executed the job an unexpected number of times.');
  }],
  '12': ['Graceful stop waits for a running handler and leaves pending work alone', async (context) => {
    const queue = open(context); const first = add(context, queue); const second = add(context, queue); const executions = [];
    const worker = queue.createWorker({ pollIntervalMs: 10 }).register('task', async (job) => { executions.push(job.id); await sleep(2000); });
    const running = start(context, worker); await until(context, () => executions.length === 1, 'The first handler did not start.');
    const began = performance.now(); let stopped = false;
    const stopping = worker.stop({ timeoutMs: 5000 }).then(() => { stopped = true; });
    await sleep(100); assert.equal(stopped, false, 'Stop returned before the running handler succeeded.');
    assert.equal(queue.getJob(first.id).status, 'active', 'The running job was completed prematurely.');
    await stopping; await running;
    context.evidence.shutdownWaitMs = Math.round(performance.now() - began); context.evidence.executionCount = executions.length;
    context.evidence.jobs = [first, second].map((job) => ({ id: job.id, status: queue.getJob(job.id).status }));
    completed([queue.getJob(first.id)]); assert.equal(queue.getJob(second.id).status, 'pending', 'Shutdown claimed additional pending work.');
    assert.deepEqual(executions, [first.id]); assert.ok(context.evidence.shutdownWaitMs >= 1800, 'Graceful stop did not wait for the two-second handler.');
  }],
  '13': ['Persistence across process restarts and valid state transitions', async (context) => {
    const written = await childResult(context, { mode: 'write' });
    const read = await childResult(context, { mode: 'read', ids: [written.job.id] });
    assert.notEqual(written.pid, read.pid); assert.deepEqual(read.job, written.job, 'Restart changed the persisted job or its payload.');
    const queue = open(context); const waiting = queue.getJob(written.job.id);
    assert.equal(queue.cancelJob(waiting.id).status, 'cancelled');
    assert.throws(() => queue.retryJob(waiting.id), /Only failed jobs/, 'A cancelled job was retried.');
    assert.throws(() => queue.cancelJob(waiting.id), /Only pending jobs/);
    const job = add(context, queue); let release;
    const gate = new Promise((resolve) => { release = resolve; }); context.release.push(release);
    const worker = queue.createWorker({ pollIntervalMs: 10 }).register('task', () => gate);
    const running = start(context, worker); await until(context, () => queue.getJob(job.id).status === 'active', 'Pending job did not become active.');
    assert.throws(() => queue.cancelJob(job.id), /Only pending jobs/, 'An active job was cancelled as if it were pending.');
    assert.throws(() => queue.retryJob(job.id), /Only failed jobs/, 'An active job was manually retried.');
    release(); completed(await terminal(context, queue, [job.id])); await worker.stop(); await running;
    assert.throws(() => queue.retryJob(job.id), /Only failed jobs/, 'A completed job was manually retried.');
    assert.throws(() => queue.cancelJob(job.id), /Only pending jobs/); await queue.close();
    const final = await childResult(context, { mode: 'read', ids: [job.id] }); completed([final.job]);
    const db = new Database(context.database, { readonly: true, fileMustExist: true });
    let integrity;
    try { integrity = db.pragma('integrity_check'); } finally { db.close(); }
    assert.deepEqual(integrity, [{ integrity_check: 'ok' }], 'SQLite integrity check failed.');
    context.evidence.jobIds.unshift(waiting.id); context.evidence.writerPid = written.pid; context.evidence.readerPids = [read.pid, final.pid];
    context.evidence.persistedPayload = read.job.data; context.evidence.transitions = ['pending → cancelled', 'pending → active → completed'];
    context.evidence.invalidTransitionsRejected = 6; context.evidence.integrity = integrity;
  }],
};

async function verify(id, directory) {
  const [title, execute] = cases[id];
  const context = { database: join(directory, `case-${id}.db`), evidence: {}, queues: [], stores: [], children: [], release: [], errors: [] };
  const began = performance.now(); let failure;
  try { await execute(context); if (context.errors.length) throw context.errors[0]; }
  catch (error) { failure = error; }
  finally {
    for (const release of context.release) release();
    for (const worker of context.children) {
      if (!worker.closed) {
        try { await worker.kill(); } catch (error) { failure ??= error; }
      }
    }
    const closing = await Promise.allSettled(context.queues.map((queue) => queue.close({ timeoutMs: 1000 })));
    for (const result of closing) if (result.status === 'rejected') failure ??= result.reason;
    for (const storage of context.stores) {
      try { storage.close(); } catch (error) { failure ??= error; }
    }
  }
  console.log(`${failure ? 'FAIL' : 'PASS'} ${id} - ${title}`);
  console.log(JSON.stringify({ database: context.database, elapsedMs: Math.round(performance.now() - began), ...context.evidence,
    ...(failure ? { error: failure.message, details: failure.stack } : {}) }, null, 2));
  return !failure;
}

try {
  const { values, positionals } = parseArgs({ options: { case: { type: 'string' } }, strict: true, allowPositionals: true });
  // npm.ps1 can consume the separator and expose --case as npm_config_case instead.
  const requested = values.case ?? (process.env.npm_config_case === undefined ? undefined : positionals.length === 1 ? positionals.shift() : process.env.npm_config_case);
  if (positionals.length) throw new Error('Unexpected arguments. Use --case followed by 01 through 13.');
  if (requested !== undefined && !Object.hasOwn(cases, requested)) throw new Error('Choose a case from 01 through 13.');
  const selected = requested === undefined ? Object.keys(cases).sort() : [requested];
  const directory = mkdtempSync(join(tmpdir(), 'queuelite-stage2-'));
  console.log(`Isolated verification databases: ${directory}`);
  let passed = 0;
  for (const id of selected) if (await verify(id, directory)) passed++;
  console.log(`${passed === selected.length ? 'PASS' : 'FAIL'} TOTAL: ${passed}/${selected.length}. Temporary databases retained for inspection.`);
  process.exitCode = passed === selected.length ? 0 : 1;
} catch (error) {
  console.error(`FAIL - ${error.message}\nUsage: npm run verify:stage2 -- --case 01 (01–13); omit --case to run all cases.`);
  process.exitCode = 2;
}
