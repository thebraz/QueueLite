import { setTimeout as sleep } from 'node:timers/promises';
import { createQueue } from '../../dist/index.js';

const config = JSON.parse(process.argv[2]);
const send = (type, evidence = {}) => process.send({ type, ...evidence });
const go = new Promise((resolve) => process.once('message', resolve));
// Do not leave verification workers behind when their runner disappears.
process.once('disconnect', () => process.exit(1));
let queue;
try {
  queue = createQueue({ database: config.database });
  send('ready', { pid: process.pid });
  await go;
  if (config.mode === 'write') {
    send('done', { job: queue.add('task', { persisted: ['UTC', 123, true] }, { runAt: Date.now() + 60000 }) });
  } else if (config.mode === 'read') {
    send('done', { job: queue.getJob(config.ids[0]) });
  } else if (config.mode === 'produce') {
    const ids = [];
    let busyRetries = 0;
    for (let i = 0; i < 20; i++) {
      const deadline = Date.now() + 5000;
      while (true) {
        try {
          ids.push(queue.add('task', { producer: process.pid }, { idempotencyKey: 'stage2-concurrent-key' }).id);
          break;
        } catch (error) {
          if (!error.code?.startsWith('SQLITE_BUSY') || Date.now() >= deadline) throw error;
          busyRetries++;
          await sleep(10);
        }
      }
      await sleep(0);
    }
    send('done', { ids, busyRetries });
  } else {
    const executions = [];
    const worker = queue.createWorker({ pollIntervalMs: 10, leaseDurationMs: config.leaseDurationMs ?? 5000 });
    worker.register('task', async (job) => {
      const execution = { id: job.id, attempt: job.attempts, pid: process.pid, startedAt: Date.now() };
      executions.push(execution);
      send('claimed', execution);
      if (config.mode === 'crash') await new Promise(() => {});
      else await sleep(config.durationMs ?? 100);
      execution.finishedAt = Date.now();
    });
    const running = worker.start();
    let engineError;
    void running.catch((error) => { engineError = error; });
    const deadline = Date.now() + 20000;
    while (config.ids.some((id) => !['completed', 'failed'].includes(queue.getJob(id)?.status))) {
      if (engineError) throw engineError;
      if (Date.now() >= deadline) throw new Error('Worker did not finish the jobs within 20 seconds.');
      await sleep(10);
    }
    await worker.stop({ timeoutMs: 1000 });
    await running;
    send('done', { executions, jobs: config.ids.map((id) => queue.getJob(id)) });
  }
} catch (error) {
  send('error', { message: error.message, stack: error.stack });
  process.exitCode = 1;
} finally {
  try { await queue?.close({ timeoutMs: 1000 }); }
  catch (error) { send('error', { message: `Could not close verification database: ${error.message}` }); process.exitCode = 1; }
  process.removeAllListeners('disconnect');
  if (process.connected) process.disconnect();
}
