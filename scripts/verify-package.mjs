import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { cpSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('..', import.meta.url));
const { name: packageName, version: packageVersion } = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));
const directory = mkdtempSync(join(tmpdir(), 'queuelite-package-'));
function run(command, args, cwd = root, expected = 0) {
  const result = spawnSync(command, args, { cwd, encoding: 'utf8', timeout: 90000, windowsHide: true });
  assert.equal(result.status, expected, `${command} failed: ${result.error?.message ?? ''}\n${result.stderr}\n${result.stdout}`);
  return result.stdout.trim();
}
try {
  assert.ok(process.env.npm_execpath, 'Run this check with npm run verify:package.');
  const [packed] = JSON.parse(run(process.execPath, [process.env.npm_execpath, 'pack', '--ignore-scripts', '--json', '--pack-destination', directory]));
  assert.ok(packed.files.some((file) => file.path === 'dist/cli.js'));
  assert.ok(packed.files.some((file) => file.path === 'dist/index.d.ts'));
  assert.ok(packed.files.some((file) => file.path === 'LICENSE'));
  assert.ok(packed.files.every((file) => /^(?:dist\/[\w-]+\.(?:js|d\.ts)|package\.json|README\.md|LICENSE)$/.test(file.path)), 'Unexpected published file.');
  const consumer = join(directory, 'consumer');
  mkdirSync(consumer);
  writeFileSync(join(consumer, 'package.json'), JSON.stringify({ private: true, type: 'module' }));
  run(process.execPath, [process.env.npm_execpath, 'install', '--no-audit', '--no-fund', '--logs-max=0', '--fetch-retries=0', '--fetch-timeout=30000', process.env.QUEUELITE_TEST_PACKAGE ?? join(directory, packed.filename)], consumer);
  const installed = join(consumer, 'node_modules', ...packageName.split('/'));
  const manifest = JSON.parse(readFileSync(join(installed, 'package.json'), 'utf8'));
  assert.equal(manifest.name, packageName);
  assert.equal(manifest.version, packageVersion);
  assert.equal(manifest.type, 'module');
  assert.deepEqual(manifest.exports, { '.': { types: './dist/index.d.ts', import: './dist/index.js' } });
  assert.equal(manifest.license, 'MIT');
  assert.equal(manifest.bin.queuelite, './dist/cli.js');
  assert.match(readFileSync(join(installed, manifest.bin.queuelite), 'utf8'), /^#!\/usr\/bin\/env node/);
  writeFileSync(join(consumer, 'consumer.ts'), `
    import { createQueue, type QueueStats, type JobAttempt, type Page } from '@thebraz/queuelite';
    interface Tasks { email: { userId: string }; count: number }
    const queue = createQueue<Tasks>({ database: ':memory:', logger: { info: event => { const id: string = event.jobId; void id; } } });
    const added = queue.add('email', { userId: 'demo' });
    const name: 'email' = added.name; void name;
    const inspected = queue.getJob(added.id);
    if (inspected?.name === 'email') { const userId: string = inspected.data.userId; void userId; }
    if (inspected?.name === 'count') { const count: number = inspected.data; void count; }
    const stats: QueueStats = queue.getStats(); void stats;
    const history: Page<JobAttempt> = queue.getAttempts(added.id); void history;
    queue.createWorker().register('email', (job, { signal }) => { const userId: string = job.data.userId; void userId; void signal; });
    // @ts-expect-error Payload must match the registered name.
    queue.add('email', 123);
    // @ts-expect-error Unknown task names are rejected.
    queue.add('missing', {});
    void queue.close();
  `);
  run(process.execPath, [join(root, 'node_modules', 'typescript', 'bin', 'tsc'), '--strict', '--noEmit', '--module', 'NodeNext', '--moduleResolution', 'NodeNext', '--target', 'ES2022', 'consumer.ts'], consumer);
  run(process.execPath, ['--input-type=module', '--eval', `
    import assert from 'node:assert/strict';
    import { createRequire } from 'node:module';
    import { createQueue, diagnose, QueueLiteError, Worker } from '@thebraz/queuelite';
    assert.throws(() => createRequire(import.meta.url)('${packageName}'), { code: 'ERR_PACKAGE_PATH_NOT_EXPORTED' });
    const queue = createQueue({ database: './consumer.db' });
    try {
      const job = queue.add('task', { token: 'packaged-payload-canary' });
      const worker = queue.createWorker().register('task', () => {});
      assert.ok(worker instanceof Worker); await worker.drain();
      assert.equal(queue.getJob(job.id).status, 'completed');
      assert.equal(queue.getAttempts(job.id).items[0].outcome, 'completed');
      assert.equal(queue.getStats().completed, 1);
      assert.equal(diagnose({ database: './consumer.db' }).ok, true);
      assert.ok(QueueLiteError.prototype instanceof Error);
    } finally { await queue.close(); }
  `], consumer);
  const cli = join(installed, 'dist', 'cli.js');
  for (const command of ['stats', 'list', 'doctor']) {
    const result = JSON.parse(run(process.execPath, [cli, command, '--db', join(consumer, 'consumer.db'), '--json'], consumer));
    assert.ok(result);
    assert.ok(!JSON.stringify(result).includes('packaged-payload-canary'));
  }
  run(process.execPath, [cli, '--help'], consumer);
  assert.match(run(process.execPath, [process.env.npm_execpath, 'exec', '--offline', '--', 'queuelite', '--help'], consumer), /queuelite <command>/);
  cpSync(join(root, 'examples'), join(consumer, 'examples'), { recursive: true });
  for (const example of ['welcome-email', 'account-provisioning', 'retry-recovery', 'delayed-job']) {
    assert.match(run(process.execPath, [join(consumer, 'examples', `${example}.mjs`)], consumer), /PASS/);
  }
  for (let runNumber = 0; runNumber < 2; runNumber++) {
    assert.match(run(process.execPath, [join(consumer, 'examples', 'welcome-email.mjs'), join(consumer, 'welcome.db')], consumer), /PASS/);
  }
  console.log('PASS Package tarball: ESM public import, persisted job, CLI, declarations, safe contents and all four examples against the installed package.');
  console.log('Fresh installation on this host passed with normal npm installation; CommonJS is not exported.');
} finally {
  assert.ok(directory.startsWith(join(tmpdir(), 'queuelite-package-')), 'Refusing cleanup outside the owned temporary directory.');
  rmSync(directory, { recursive: true, force: true });
}
