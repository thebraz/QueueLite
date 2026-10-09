import assert from 'node:assert/strict';
import { createQueue } from '@thebraz/queuelite';
import { waitForJob } from './wait-for-job.mjs';

const queue = createQueue({ database: ':memory:' });
try {
  let deliveredAt = 0;
  const worker = queue.createWorker({ pollIntervalMs: 10 }).register('notification', () => { deliveredAt = Date.now(); });
  const job = queue.add('notification', { userId: 'demo-user' }, { delay: 150 });
  assert.equal(queue.getStats().delayed, 1);
  await waitForJob(queue, worker, job.id);
  assert.ok(deliveredAt >= job.runAt);
  console.log('PASS Delayed job: notification delivered after its scheduled time.');
} finally { await queue.close({ timeoutMs: 1000 }); }
