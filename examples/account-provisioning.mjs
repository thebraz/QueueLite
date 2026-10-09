import assert from 'node:assert/strict';
import { createQueue } from '@thebraz/queuelite';
import { waitForJob } from './wait-for-job.mjs';

const queue = createQueue({ database: ':memory:' });
try {
  const account = { id: 'demo-account', state: 'pending', prepared: false, emailVerified: false, authorized: false };
  const worker = queue.createWorker({ pollIntervalMs: 10 }).register('prepare-account', () => { account.prepared = true; });
  // A verified payment event prepares an account; it does not authorize activation.
  const job = queue.add('prepare-account', { accountId: account.id }, { idempotencyKey: 'payment-demo:prepare' });
  await waitForJob(queue, worker, job.id);
  function activate() {
    if (!account.prepared || !account.emailVerified || !account.authorized) return false;
    account.state = 'active'; return true;
  }
  assert.equal(activate(), false);
  assert.equal(account.state, 'pending');
  account.emailVerified = true; // Simulated independent email verification.
  assert.equal(activate(), false);
  account.authorized = true; // Simulated independent authorization.
  assert.equal(activate(), true);
  console.log('PASS Account provisioning: activation required preparation, verification and authorization.');
} finally { await queue.close({ timeoutMs: 1000 }); }
