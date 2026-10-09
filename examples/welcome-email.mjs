import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { createQueue } from '@thebraz/queuelite';
import { waitForJob } from './wait-for-job.mjs';

const queue = createQueue({ database: process.argv[2] ?? ':memory:' });
try {
  // Simulate registration, then enqueue an email without external credentials.
  const registeredUser = { id: `demo-${randomUUID()}`, emailVerified: false };
  const sent = new Set();
  const worker = queue.createWorker({ pollIntervalMs: 10 }).register('welcome-email', (job) => {
    // In production, pass job.id as the provider's idempotency key.
    sent.add(job.id);
  });
  const job = queue.add('welcome-email', { userId: registeredUser.id }, { idempotencyKey: `${registeredUser.id}:welcome` });
  await waitForJob(queue, worker, job.id);
  assert.equal(sent.has(job.id), true);
  assert.equal(registeredUser.emailVerified, false);
  console.log('PASS Welcome email: registered user received a simulated email.');
} finally { await queue.close({ timeoutMs: 1000 }); }
