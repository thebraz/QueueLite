import { expect, it } from 'vitest';
import { createQueue } from '../src/index.js';

it('keeps payload types associated with registered names', async () => {
  interface EmailPayload { userId: string }
  interface Tasks { email: EmailPayload; count: number }
  const queue = createQueue<Tasks>({ database: ':memory:' });
  const job = queue.add('email', { userId: 'user_123' });
  const worker = queue.createWorker();
  worker.register('email', (received) => { expect(received.data.userId).toBe('user_123'); });
  // These calls are checked by tsc but are never executed.
  if (false) {
    // @ts-expect-error Unknown job name.
    queue.add('unknown', {});
    // @ts-expect-error Payload does not match the job name.
    queue.add('email', 123);
    // @ts-expect-error Handler payload does not match the job name.
    worker.register('count', (received: { data: string }) => { void received; });
  }
  expect(job.data.userId).toBe('user_123');
  const inspected = queue.getJob(job.id);
  if (inspected?.name === 'email') expect(inspected.data.userId).toBe('user_123');
  if (inspected?.name === 'count') { const count: number = inspected.data; void count; }
  await queue.close();
});
