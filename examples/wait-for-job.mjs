import { setTimeout as sleep } from 'node:timers/promises';

// drain() handles currently eligible jobs; this helper also waits for future retries/delays.
export async function waitForJob(queue, worker, id) {
  const running = worker.start();
  let engineError;
  void running.catch((error) => { engineError = error; });
  try {
    const deadline = Date.now() + 5000;
    while (Date.now() < deadline) {
      if (engineError) throw engineError;
      const job = queue.getJob(id);
      if (job.status === 'completed') return job;
      if (job.status === 'failed' || job.status === 'cancelled') throw new Error('Example job did not complete; inspect its failure history.');
      await sleep(10);
    }
    throw new Error('Example timed out.');
  } finally { await worker.stop({ timeoutMs: 1000 }); await running; }
}
