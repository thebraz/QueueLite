import { createQueue } from '../../dist/index.js';
import Database from 'better-sqlite3';
import { setTimeout as sleep } from 'node:timers/promises';

const [mode, database, ...ids] = process.argv.slice(2);
const send = (type, data = {}) => process.send({ type, ...data });
const message = () => new Promise((resolve) => process.once('message', resolve));
if (mode === 'lock') {
  const db = new Database(database);
  try {
    db.exec('BEGIN IMMEDIATE'); send('locked');
    await message(); db.exec('COMMIT'); send('released');
  } finally { db.close(); process.disconnect(); }
} else {
  const queue = createQueue({ database });
  try {
    if (mode === 'produce') {
      const go = message(); send('ready'); await go;
      const jobs = [];
      for (let i = 0; i < 30; i++) {
        const deadline = Date.now() + 5000;
        while (true) {
          try { jobs.push(queue.add('task', { producer: process.pid }, { idempotencyKey: 'concurrent-key' }).id); break; }
          catch (error) {
            if (!error.code?.startsWith('SQLITE_BUSY') || Date.now() > deadline) throw error;
            await sleep(2);
          }
        }
      }
      send('produced', { ids: jobs });
    } else {
      // Only the crashed/stalled owner needs a short lease; the replacement must tolerate CI scheduling delays.
      const worker = queue.createWorker({ pollIntervalMs: 5, leaseDurationMs: mode === 'recover' ? 30000 : 250,
        heartbeatIntervalMs: mode === 'recover' ? 10000 : 50, concurrency: 3 });
      const seen = [];
      worker.register('task', async (job) => {
        seen.push(job.id); send('claimed', { id: job.id, attempt: job.attempts });
        if (mode === 'crash') await new Promise(() => {});
        else if (mode === 'stale') {
          const command = message(); send('ready'); await command;
          send('stalled');
          // Deliberately freeze this process beyond its lease; another process stays responsive.
          Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 1500);
        } else await sleep(10);
      });
      const running = worker.start();
      const done = (async () => {
        const deadline = Date.now() + 10000;
        while (ids.some((id) => !['completed', 'failed'].includes(queue.getJob(id).status))) {
          if (Date.now() > deadline) throw new Error('Process recovery timed out.');
          await sleep(10);
        }
      })();
      await Promise.race([done, running]); await worker.stop(); await running;
      send('done', { seen, jobs: ids.map((id) => queue.getJob(id)) });
    }
  } finally { await queue.close(); if (process.connected) process.disconnect(); }
}
