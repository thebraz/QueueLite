# QueueLite

A persistent background job queue for Node.js and TypeScript, backed by a local SQLite file. QueueLite keeps scheduled work and retry state across application restarts without requiring Redis or a database server. Version 0.1.0 is an initial experimental release; validate it against your workload before production use.

## Features

- Typed ESM SDK and a local inspection CLI.
- Atomic job claims, bounded handler concurrency, priority and persistent scheduling.
- Fixed/exponential retries with jitter and retained failure history.
- Persistent enqueue deduplication, renewable leases and bounded crash recovery.
- Graceful shutdown, pending-job cancellation, paginated inspection and diagnostics.

## Installation

Use Node.js 22.14 or newer and npm. The package exports **ES modules only**; CommonJS `require()` is not supported. `better-sqlite3` is the only direct runtime dependency and includes native code; platform support depends on its native binaries or available build tools.

```sh
npm install @thebraz/queuelite
```

The executable remains `queuelite`. Package examples are available in this repository, not included in the npm tarball.

## Local development

Use Node.js 22.14 or newer on a supported Node.js release line, and npm.

```sh
npm ci
npm run typecheck
npm run lint
npm run build
npm test
npm run examples
npm run verify:package
```

The examples simulate welcome email, account provisioning, retry/recovery and a delayed notification, without credentials. They use isolated in-memory queues by default. `npm run example -- ./another.db` opts into a persistent welcome-email database; create its parent directory first. Existing files are preserved, but writable SDK access upgrades old schemas. Process tests exercise the built SDK; `npm test` builds before running them. `verify:package` packs and installs into a temporary consumer, checks public imports and TypeScript declarations, invokes the installed CLI and runs all four repository examples against the installed package. It requires npm registry access (or a populated cache), uses normal npm installation and removes only its own temporary consumer.

## First job

Save this as `quick-start.mjs` after installing the package, then run `node quick-start.mjs`. It also runs from a built repository checkout. A local release candidate can be installed with `npm install /path/to/thebraz-queuelite-0.1.0.tgz` before registry publication.

```js
import { createQueue } from '@thebraz/queuelite';

const queue = createQueue({ database: './queuelite.db' });
try {
  const worker = queue.createWorker().register('welcome-email', () => {
    console.log('Simulated email sent.');
  });
  const job = queue.add('welcome-email', { userId: 'demo' });
  await worker.drain();
  console.log(queue.getJobSummary(job.id));
} finally { await queue.close(); }
```

`drain()` handles currently eligible work; delayed jobs and future retries need a running worker. The examples include a bounded wait for their own job. Account provisioning prepares a pending account, then independently checks email verification and authorization before activation. The in-memory account and simulated email provider are demonstrations, not an application database/outbox transaction or a real email integration.

## Public API

```ts
import { createQueue } from '@thebraz/queuelite';

const queue = createQueue<{
  'send-email': { userId: string; template: string };
}>({ database: './queuelite.db' });

const worker = queue.createWorker({ pollIntervalMs: 100, leaseDurationMs: 30000 });
worker.register('send-email', async (job, { signal }) => {
  // Pass signal to cancellable I/O; make external effects idempotent using job.id.
  if (signal.aborted) return;
  console.log(`Simulated email for ${job.data.userId}: ${job.data.template}`);
});

const job = queue.add('send-email', { userId: 'user_123', template: 'welcome' }, {
  attempts: 4,
  backoff: { type: 'exponential', delay: 1000, jitter: 0.2 },
  delay: 60000,
  priority: 10,
  idempotencyKey: 'user_123_welcome',
});
const running = worker.start({ concurrency: 5 });
// Observe engine failures immediately without logging raw causes or payloads.
void running.catch(() => { console.error('Queue worker stopped; inspect storage locally.'); });
console.log(queue.getJobSummary(job.id));
// Later, on application shutdown, outside handlers:
await queue.close({ timeoutMs: 10000 });
await running;
```

An optional task map connects names to payload types; otherwise payloads use the exported `JsonValue` type. `add`, lookup, cancellation, and manual retry are synchronous; awaiting their results also works. Existing one-argument handlers, `runAt`, serial `start()`, `stop()` and `close()` calls remain supported.

| Method | Behavior |
| --- | --- |
| `createQueue({ database })` | Opens a dedicated local SQLite file, initializes or migrates its schema. `:memory:` is ephemeral. |
| `queue.add(name, data, options?)` | Persists a new job or returns the existing job for a duplicate effective idempotency key. |
| `queue.getJob(id)` | Returns a fresh persisted snapshot or `undefined`. |
| `queue.getJobSummary(id)` | Payload-free metadata or `undefined`; also omits errors and idempotency keys. |
| `queue.listJobs({ status?, limit?, after? })` | Bounded metadata page in insertion order. `pending` includes delayed jobs; `delayed` selects future pending jobs. |
| `queue.getStats()` | Current disjoint job counts, historical attempt outcomes, retry executions and approximate mean duration. |
| `queue.getAttempts(id, { limit?, after? })` | Bounded persistent attempt page, including running, completed, failed and recovered attempts. |
| `queue.getFailureHistory(id, { limit?, after? })` | Attempt page restricted to recorded failures/recoveries, with redacted error details. |
| `queue.getActiveClaims({ limit?, after? })` | Bounded active job metadata, including lease expiration, without ownership tokens. |
| `diagnose({ database })` | Reads existing storage without creating, migrating, recovering or repairing it. |
| `queue.getFailedJobs(limit = 100)` | Returns terminal failures in insertion order; limit is 1–1000. |
| `queue.retryJob(id)` | Only for terminal failures: resets attempts to zero and schedules immediately, retaining history and configuration. |
| `queue.cancelJob(id)` | Only for pending jobs, including delayed/retry jobs; terminally cancels them atomically. |
| `queue.createWorker(options?)` | Creates a stopped worker. Defaults: concurrency 1, poll 100 ms, lease 30 s, heartbeat every lease/3 (rounded down). |
| `worker.register(name, handler)` | Registers a sync/async handler once per name. Optional second argument contains `signal`. |
| `worker.start({ concurrency? })` | Returns the worker lifetime promise, resolving on shutdown and rejecting on fatal storage/engine errors. Overrides configured concurrency for this run. |
| `worker.pause()` / `resume()` | Pauses/resumes new claims; active handlers and heartbeats continue. Starting a stopped worker clears pause. |
| `worker.stop({ timeoutMs? })` | Stops new claims, wakes idle polling, and awaits active handlers and their state writes. |
| `worker.drain({ timeoutMs? })` | Starts/resumes processing if needed, handles eligible jobs and expired claims, then stops when it observes no eligible or active work. |
| `queue.close({ timeoutMs? })` | Prevents further SDK operations, stops owned workers, and closes SQLite even after a shutdown deadline. Repeated calls return the same promise. |

Worker options additionally accept `heartbeatIntervalMs` (positive and less than the lease), `shutdownTimeoutMs` (default deadline for stop/drain/close), and `concurrency` (1–1000). Lease duration must be 3–2147483647 ms; practical leases should be substantially longer than expected event-loop and disk stalls. Poll and heartbeat intervals must be positive timer-safe integers. Defaults impose no shutdown deadline for backward compatibility.

Queue options also accept `readOnly: true` for inspecting an existing schema-3 file and `fileMustExist: true` to prevent accidental database creation. Read-only queues cannot create workers or change jobs. Opening older schemas read-only fails with explicit upgrade instructions. `onEvent` and `logger` are described below. Queue storage errors are `QueueLiteError` instances with a safe operation message, a `retryable` flag and a SQLite `code` when available. The original `cause` is available for deliberate local debugging; do not serialize it to public logs. Validation/state errors are not automatically retryable. Job lookup returns a discriminated `TypedJob<T>` union: checking `job.name` narrows `job.data` to the matching task payload.

## Inspection and pagination

Pages are `{ items, nextCursor }`. The default limit is 100, maximum 1000; limits must be positive integers. Pass the numeric `nextCursor` as `after` to continue, keeping the same filter and job ID. Cursors are monotonically increasing insertion/attempt sequence numbers, not timestamps or offsets. A null cursor ends the observed page sequence. New jobs may appear on later pages; concurrent transitions mean pages are live observations, not a snapshot spanning multiple calls. Unknown job IDs return empty attempt/history pages. Use `getJobSummary` to distinguish a missing job from a job with no attempts.

```js
const first = queue.listJobs({ status: 'failed', limit: 25 });
if (first.nextCursor !== null) {
  const next = queue.listJobs({ status: 'failed', limit: 25, after: first.nextCursor });
  console.log(next);
}
console.log(queue.getAttempts(job.id, { limit: 10 }));
```

List and summary queries never select payloads or the legacy JSON error-history array. Attempts and failures use indexed, bounded queries. `getJob` and the compatible `getFailedJobs` return full snapshots and should be used deliberately when payload/history access is needed. Statistics use SQL aggregates rather than loading job rows into JavaScript; computing totals still scans retained records, so poll at a practical interval.

Current statistics split stored pending jobs into `pending` (due now) and `delayed` (future). These and active/completed/failed/cancelled sum to `total`. `retryAttempts` counts actual claims after a job's first historical execution, including a new execution after manual retry; scheduling a retry alone does not increase it. `outcomes` counts individual completed, failed and recovered attempts, independently of the current terminal job state. A recovered attempt may ultimately produce a terminal failed job. `averageDurationMs` averages finished attempts with known starts, clamps clock reversals to zero and includes recovery detection delay. It is approximate wall-clock time, not CPU time. All counters persist across restarts.

## CLI and diagnostics

Use `npx --no-install queuelite` in a project that has installed the package, or `queuelite` after a global installation. From a built repository checkout, use `node dist/cli.js`.

```sh
queuelite stats --db ./queuelite.db
queuelite list --db ./queuelite.db --status failed --limit 25 --json
queuelite list --db ./queuelite.db --limit 25 --after 25
queuelite inspect <job-id> --db ./queuelite.db --limit 10 --json
queuelite retry <job-id> --db ./queuelite.db
queuelite cancel <job-id> --db ./queuelite.db
queuelite doctor --db ./queuelite.db --json
queuelite --help
```

The database defaults to `./queuelite.db`, resolved from the working directory. Every command requires an existing file; none creates one. `stats`, `list` and `inspect` open read-only. `retry` and `cancel` first verify compatibility read-only, then use the same atomic SDK state transitions; they do not implicitly upgrade an old file. `--status` is exclusive to `list`; `--limit` and `--after` apply to `list` and `inspect`. The latter returns independently paginated `attempts` and `failures`, whose cursors refer to the same attempt sequence. CLI output omits payloads, idempotency keys and free-form failure messages, including on retry/cancel; use the SDK deliberately for detailed errors. Human output escapes stored strings to prevent terminal-control injection. `--json` emits exactly one JSON value, including errors. Exit codes: **0** success/help, **1** storage/state failure or unhealthy diagnostics, **2** invalid arguments, **3** unknown job ID. Diagnostic warnings also return 1.

`doctor` checks configuration, existing-file accessibility, file/directory write permissions, schema version/required columns, SQLite `quick_check(1)`, queue/attempt consistency and expired leases. It reads a single snapshot and makes no repair, migration or recovery writes. Permission checks do not guarantee available disk space, future write success or hardware durability. A valid lease cannot establish whether its worker is alive. For expired claims, explicitly start a worker with all required handlers; normal lease recovery applies the existing retry budget. Preserve a backup before investigating damaged storage, and restore a verified backup instead of overwriting it. Older schemas produce backup/upgrade instructions without changing the version or data.

## Lifecycle and structured logging

```js
const queue = createQueue({
  database: './queuelite.db',
  onEvent(event) { /* send safe counters to your metrics integration */ },
  logger: { info(event) { console.log(JSON.stringify(event)); } },
});
```

Event types are `enqueued`, `started`, `completed`, `failed`, `retried`, `recovered` and `cancelled`. Each immutable event contains only `type`, `jobId`, `at`, `attempt`, `status`, plus approximate `durationMs` on completion. Job names, payloads, keys, error strings and lease tokens are excluded. Duplicate idempotent submissions do not emit another enqueue event; rejected or rolled-back transitions emit none. A transient failure emits failed and retried; lease recovery additionally emits recovered. Manual retry emits retried with the reset attempt count.

Hooks run after successful commits, in transition order. Recovery may commit several transitions together; inspecting the job from a hook can therefore observe a later state in the same committed transaction. Events describe operations performed by this queue/connection, not every process sharing the database; they are neither a durable event stream nor a replay mechanism. Use persisted attempts for historical outcomes. Hook/logger exceptions and rejected promises are isolated from processing and do not trigger job retries. Keep hooks fast and nonblocking: slow synchronous logging can stall heartbeats. No logger is enabled by default, and observer delivery failures are the integration's responsibility.

Jobs include the original `id`, `name`, `data`, `status`, `createdAt`, `updatedAt`, `attempts`, `runAt`, `startedAt`, `finishedAt`, `error`, plus `maxAttempts`, `backoff`, `priority`, `idempotencyKey`, `errorHistory`, and `leaseExpiresAt`. Lease tokens stay inside the engine. Snapshots are mutable copies; handler mutations cannot change the claimed identity, token, or attempt used for writes. Handlers return no stored result.

Payloads must be plain JSON data: null, strings, booleans, finite numbers, dense arrays, and plain objects. Functions, undefined, bigint, dates, cycles, symbols, accessors, hidden properties, and extra array properties are rejected. Names must be trimmed, non-empty strings of at most 255 UTF-16 code units without null bytes. Applications must validate external producer inputs themselves.

## Scheduling, priority and retries

All timestamps are non-negative integer UTC Unix milliseconds. `delay` is a relative non-negative integer in milliseconds; `runAt` is an absolute timestamp. Use one or the other. Both persist across restarts. A worker never claims before `runAt`; normal idle precision is approximately one polling interval plus handler capacity, event-loop latency, and storage contention. There is no hard real-time guarantee. Keep the system clock synchronized: clock jumps affect scheduling and lease expiration.

Eligible jobs are claimed in descending numeric `priority` (default 0, signed 32-bit integer), then original insertion sequence. This FIFO tie-breaker survives restarts and timestamp ties; concurrent completion order can differ. Future jobs remain ineligible regardless of priority. Constant high-priority arrivals can starve lower-priority work.

`attempts` is the **maximum total claims, including the initial claim**, from 1 to 1000, default 1. Persisted `job.attempts` counts claims in the current retry cycle; `maxAttempts` is its budget. Crashed/expired claims consume attempts too, even when a process crashes before entering its handler. A successful handler finishes the job; a failure with remaining attempts returns it to pending at a persisted retry timestamp. Exhaustion produces terminal `failed`. Missing handlers fail terminally immediately, with `kind: 'missing-handler'`, so an invalid registration cannot create an automatic retry storm. Register all names on every worker sharing the database; fix registration before manually retrying these failures.

Backoff defaults to fixed zero delay. For failed attempt `n`, fixed backoff is `delay`; exponential is `delay * 2^(n-1)`. Optional jitter `j` is 0–1: choose an integer delay in `[floor(base * (1-j)), base]` using a random reduction. Values and resulting timestamps saturate at `Number.MAX_SAFE_INTEGER`. Finite attempt budgets prevent infinite automatic retries, including when jitter or delay produces zero. Crash recovery uses the same backoff policy, measured from recovery detection.

During a temporary failure, `status` is pending, `error` and execution timestamps reset, and `errorHistory` retains the failure. Terminal failures have `error` and `finishedAt`. Each history entry records attempt, UTC time, kind, name, message, and optional string code. Manual retries retain every earlier entry, while restarting the attempt budget. History therefore can contain repeated attempt numbers from distinct manual cycles. Error messages are capped at 2048 characters, names/codes at 128; arbitrary objects, getters, stacks, causes and custom metadata are not persisted. Common labelled credentials, authorization values and URL passwords are redacted on a best-effort basis. Free-form messages may still contain sensitive information: never throw secrets and protect database access. The SDK does not log handler errors automatically.

## Deduplication and execution guarantees

The effective idempotency key is `(job name, idempotencyKey)` within one database, enforced by a unique partial SQLite index and an immediate insertion transaction. The key is a non-empty string of at most 255 UTF-16 code units without null bytes. Two names can share a key. A duplicate returns the original job, including its original payload/options and current state. Keys remain reserved through success, failure, cancellation, restarts and manual retries; there is no TTL or deletion API. Without a key, each enqueue creates a new UUID.

Processing has **at-least-once-style recovery, bounded by a finite claim budget**. It does not guarantee eventual handler invocation, successful delivery or exactly-once external effects. A crash after an external effect but before completion can repeat that effect on recovery. Default attempts=1 makes an abandoned job terminally failed after expiration; configure additional attempts for automatic re-execution. External service handlers must enforce their own idempotency, preferably using the stable job ID or domain operation key. An expired or cancelled handler cannot have its external effects rolled back by this SDK.

Every claim has a random token and expiration. Claiming, recovery, and final writes run in short immediate transactions. Heartbeats and completion/failure require the same token, active state, and an unexpired lease, so an expired lease cannot be renewed or completed by its former owner. Recovery records `lease-expired` and retries or fails according to the budget, in bounded batches of 100 per claim transaction. Workers recover during claim polling; no separate daemon is needed. Recovery requires an available worker, a released database lock, and free worker capacity. A paused/stopped worker does not recover jobs.

State transitions are `pending → active → completed`, `active → pending` for retry/recovery, `active → failed` for terminal errors, `failed → pending` for explicit manual retry, and `pending → cancelled`. Completed and cancelled jobs cannot be retried. Terminal states and history are inspectable. Only one live claim owns a job at a time; an expired handler may continue executing user code concurrently with its replacement, but cannot overwrite the replacement's state.

## Shutdown and cancellation

Normal stop drains only already-active handlers; pending work remains. `drain` additionally processes eligible work, including newly due retries, until an empty observation. It does not wait for future delayed jobs/retries or guarantee inclusion of work submitted after the empty observation. Pausing does not interrupt active work. Stopping one queue does not stop workers from another queue instance.

A deadline stops heartbeats, aborts handler signals, abandons unfinished claims for lease recovery, and rejects stop/drain/close with `QueueLiteError`. It never marks unfinished work completed. The worker lifetime promise resolves for this deliberate abandonment; fatal engine errors reject it. A late handler cannot write to a closed database or overwrite recovered state. Restarting that worker is rejected until its abandoned handlers settle, preserving its concurrency bound.

Cancellation is cooperative: handlers should pass `signal` to cancellable I/O and check it before external effects. Lease loss also aborts the signal. JavaScript cannot force a promise or external service to stop. Synchronous CPU work blocks timers, heartbeats and deadlines; use application-managed processes/threads for such work. A handler that ignores cancellation may retain memory/resources until it settles or the process exits. No per-job execution timeout is implemented. Request shutdown from outside handlers; awaiting a worker's stop/drain or queue close from its own handler would wait for itself.

## Architecture, migration and SQLite limits

- `src/queue.ts`: typed public SDK, input validation, inspection, manual retry/cancellation, connection shutdown.
- `src/worker.ts`: bounded handler concurrency, polling, heartbeat ownership, lifecycle and cooperative cancellation.
- `src/storage.ts`, `src/schema.ts`: schema version 3, transactional migration, persistent FIFO/attempt sequences, indexed inspection, unique keys, atomic claims, bounded recovery, fenced transitions.
- `src/cli.ts`, `src/diagnostics.ts`: SDK-backed terminal commands and non-mutating diagnostics.
- `src/reliability.ts`: bounded backoff, safe error serialization and transient SQLite lock classification.
- `src/types.ts`, `src/validation.ts`, `src/errors.ts`: contracts, validation and SDK errors.

The only direct production dependency remains [better-sqlite3](https://github.com/WiseLibs/better-sqlite3), a synchronous native addon. Platforms without a prebuilt binary require native build tools. No transaction spans a handler. File databases use WAL and `synchronous = FULL`; memory databases do not use WAL. Durability still depends on OS/hardware guarantees. Preserve live WAL files and use SQLite-aware backups, or close all connections before copying.

Versions 1 and 2 upgrade transactionally to version 3 without replacing job identities, payloads, retry configuration, keys or terminal outcomes. Version 1 active claims become expired leases with one recovery attempt added (capped at 1000); existing error strings are preserved, but version 1 has no structured error history to import. Version 3 imports available version-2 structured failures and current active/completed attempts into the attempt ledger. Legacy terminal failures without structured history receive a placeholder attempt pointing to the preserved job error. Earlier successful/manual cycles and failure start times absent from old storage cannot be reconstructed; imported historical counters can be incomplete and unknown durations are excluded. New attempts are recorded fully. Unknown versions are rejected before changing the journal mode. Stop every old worker and back up storage before upgrading; downgrade is unsupported. Schema-2 workers cannot update the new attempt ledger, and Schema-1 workers do not implement lease ownership.

Multiple processes **on the same host and local filesystem** may share the database. Network filesystem sharing and multi-host distributed operation are unsupported. SQLite serializes writes; concurrency improves asynchronous I/O overlap, not write throughput or CPU parallelism. FULL sync, large payloads/history, many producers and scans past future high-priority jobs can increase latency. There is no universal throughput claim or production load benchmark. Retention/cleanup is not implemented: terminal jobs, keys and history accumulate, especially with repeated manual retries.

Initialization/migration may wait up to five seconds for locks. Runtime uses a zero busy timeout so SQLite cannot stall the event loop waiting for a competing writer. Workers yield and retry transient `SQLITE_BUSY*`/`SQLITE_LOCKED*` errors at bounded timer intervals, including final writes; they do not rerun a successful handler merely because completion was temporarily locked. Long contention can still expire a lease and cause recovery. Synchronous producer/manual operations fail promptly on lock contention: callers should retry transient busy errors with bounded asynchronous backoff. Other engine/database errors stop the worker, abort its claims and reject its lifetime promise. Always observe that promise and supervise/restart workers as appropriate.

## Testing

Run `npm run validate` for type checking, lint, the automated test suite, examples,
and clean consumer installation. `npm run verify:stage3` runs all 17 operational
scenarios and includes the 13 `verify:stage2` process/recovery scenarios. These
runners retain isolated temporary databases for inspection and never accept a
user database path. Use `npm run verify:stage3 -- --case 01` to select a case;
unselected scenarios are explicitly skipped.

Tests cover persistence, atomic claims across processes, concurrent producers,
retries/backoff, scheduling, migrations and rollback, killed/stalled workers,
lease fencing, lock contention, shutdown deadlines, observer failures, payload
validation, CLI arguments/output and metadata redaction. Package checks cover
emitted JavaScript/declarations, ESM imports, the installed executable, normal
native-dependency installation and examples against the installed tarball.
CommonJS is explicitly rejected by the export map.

Before production use, verify the application's real external side effects are idempotent, kill/restart workers around those effects, send the application's shutdown signal while requests are active, and measure event-loop delay, disk latency, write contention and backlog on the target machine. The library does not install process signal handlers; applications must call `stop`/`close`. Tests use simulated handlers and local SQLite, without real third-party APIs or network-filesystem validation. GitHub Actions defines checks for Windows, Linux and macOS on Node.js 22.14 and 24; only completed runs establish platform validation. There is no production throughput claim.

## Security and operational boundaries

Database files contain plaintext payloads, keys and retained errors. Restrict file and directory permissions and use trusted local paths. QueueLite does not provide encryption, authentication, tenant isolation or handler sandboxing. Do not put credentials in job names or errors. Lifecycle logs omit arbitrary names, payloads and failure messages; stored error redaction is best effort, and raw error causes are available through the SDK for deliberate local debugging.

Payloads must be finite, acyclic plain JSON. Accessors, custom object/array prototypes, sparse arrays and non-JSON values are rejected. Payload size and retained history are not capped; applications must bound input and monitor memory, disk space and backlog. Handlers share the process and event loop; CPU stalls and long SQLite contention can expire leases and repeat effects. No cleanup/retention API or distributed multi-host scaling is implemented.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Run `npm run validate` for typing, lint, tests, examples and a clean tarball consumer check. The CI workflow validates changes without automatically publishing them. Release notes are in [CHANGELOG.md](CHANGELOG.md).

## License

[MIT](LICENSE).
