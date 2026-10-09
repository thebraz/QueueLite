import { createQueue } from '../../dist/index.js';
import { setTimeout } from 'node:timers/promises';

const [mode, database, ...ids] = process.argv.slice(2);
const queue = createQueue({ database });
try {
  if (mode === 'write') {
    console.log(queue.add('task', { text: 'persisted', nested: [1, null, true] }).id);
  } else if (mode === 'read') {
    console.log(JSON.stringify(queue.getJob(ids[0])));
  } else {
    const claimed = [];
    const worker = queue.createWorker({ pollIntervalMs: 5 });
    worker.register('task', async (job) => { claimed.push(job.id); await setTimeout(10); });
    const running = worker.start();
    const done = (async () => {
      const deadline = Date.now() + 10000;
      while (ids.some((id) => queue.getJob(id).status !== 'completed')) {
        if (Date.now() > deadline) throw new Error('Process race timed out.');
        await setTimeout(10);
      }
    })();
    await Promise.race([done, running]);
    await worker.stop();
    await running;
    console.log(JSON.stringify(claimed));
  }
} finally {
  await queue.close();
}
