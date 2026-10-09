import assert from 'node:assert/strict';
import { createQueue } from '@thebraz/queuelite';
import { waitForJob } from './wait-for-job.mjs';

const queue = createQueue({ database: ':memory:' });
try {
  let calls = 0;
  const worker = queue.createWorker({ pollIntervalMs: 10 }).register('external-service', () => {
    if (++calls < 3) throw new Error('Simulated temporary service outage.');
  });
  const job = queue.add('external-service', {}, { attempts: 3, backoff: { type: 'fixed', delay: 40 } });
  const result = await waitForJob(queue, worker, job.id);
  assert.equal(result.attempts, 3);
  assert.equal(queue.getFailureHistory(job.id).items.length, 2);
  assert.equal(queue.getStats().retryAttempts, 2);
  console.log('PASS Retry and recovery: two temporary failures, then success.');
} finally { await queue.close({ timeoutMs: 1000 }); }
