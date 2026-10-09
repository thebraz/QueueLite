#!/usr/bin/env node
import { parseArgs } from 'node:util';
import { createQueue } from './queue.js';
import { diagnose } from './diagnostics.js';
import { QueueLiteError } from './errors.js';
import type { JobSummary, ListOptions } from './types.js';

const help = `QueueLite — inspect a local SQLite queue without an HTTP server

Usage: queuelite <command> [job-id] [options]

Commands:
  stats             Show queue counts, attempts and average duration
  list              List job metadata in insertion order (paginated)
  inspect <job-id>  Show metadata and paginated attempts/failure history
  retry <job-id>    Retry a terminal failed job, preserving history
  cancel <job-id>   Cancel a pending job, including delayed jobs
  doctor            Diagnose existing storage without repairing or migrating

Options:
  --db <path>       Existing SQLite file (default: ./queuelite.db)
  --status <state>  list only: pending, active, completed, failed, cancelled, delayed
  --limit <number>  list/inspect page size, 1–1000 (default: 100)
  --after <number>  list/inspect cursor from a previous page
  --json            Output one JSON value, including errors
  --help, -h        Show this help

Payloads, idempotency keys and free-form error messages are omitted.
Exit codes: 0 success; 1 operation/diagnostic failure; 2 invalid usage; 3 job not found.
Read commands require schema 3; use doctor for older or damaged databases.
`;

function metadata(job: JobSummary) {
  return { id: job.id, name: job.name, status: job.status, attempts: job.attempts, maxAttempts: job.maxAttempts,
    createdAt: job.createdAt, updatedAt: job.updatedAt, runAt: job.runAt, startedAt: job.startedAt,
    finishedAt: job.finishedAt, priority: job.priority, backoff: job.backoff, leaseExpiresAt: job.leaseExpiresAt };
}
function print(value: unknown, json: boolean): void {
  if (json) { console.log(JSON.stringify(value)); return; }
  if (value && typeof value === 'object' && 'items' in value && Array.isArray(value.items)) {
    console.log('ID                                    STATUS     ATTEMPTS  RUN AT (UTC ms)  NAME');
    for (const item of value.items as JobSummary[]) {
      console.log(`${JSON.stringify(item.id).padEnd(40)}${item.status.padEnd(11)}${`${item.attempts}/${item.maxAttempts}`.padEnd(10)}${String(item.runAt).padEnd(17)}${JSON.stringify(item.name)}`);
    }
    if (value.items.length === 0) console.log('No jobs found.');
    console.log(`nextCursor: ${'nextCursor' in value ? value.nextCursor : null}`);
    return;
  }
  if (value === null || typeof value !== 'object') { console.log(String(value)); return; }
  // Escaping strings prevents stored names or file paths from injecting terminal control sequences.
  const display = (item: unknown): string => typeof item === 'string' ? JSON.stringify(item) : JSON.stringify(item, null, 2);
  console.log(Object.entries(value).map(([key, item]) => `${key}: ${display(item)}`).join('\n'));
}
function number(value: string | undefined, label: string, min: number): number | undefined {
  if (value === undefined) return undefined;
  if (!/^\d+$/.test(value) || !Number.isSafeInteger(Number(value)) || Number(value) < min || (label === 'limit' && Number(value) > 1000)) {
    throw new QueueLiteError(`${label} must be an integer ${label === 'limit' ? 'between 1 and 1000' : 'at least 0'}.`);
  }
  return Number(value);
}

async function main(): Promise<void> {
  let json = process.argv.includes('--json');
  let usage = true;
  try {
    const { values, positionals } = parseArgs({ strict: true, allowPositionals: true, options: {
      db: { type: 'string' }, status: { type: 'string' }, limit: { type: 'string' }, after: { type: 'string' },
      json: { type: 'boolean' }, help: { type: 'boolean', short: 'h' },
    } });
    json = values.json ?? false;
    if (values.help || process.argv.length === 2) { if (json) print({ help }, true); else console.log(help); return; }
    const [command, id] = positionals;
    if (!command || !['stats', 'list', 'inspect', 'retry', 'cancel', 'doctor'].includes(command)) throw new QueueLiteError('Unknown command. Use --help for commands.');
    const needsId = ['inspect', 'retry', 'cancel'].includes(command);
    if (positionals.length !== (needsId ? 2 : 1) || (needsId && !id?.trim())) throw new QueueLiteError(`${command}${needsId ? ' requires one job ID' : ' takes no job ID'}. Use --help.`);
    if (values.status !== undefined && command !== 'list') throw new QueueLiteError('--status is supported only by list.');
    if ((values.limit !== undefined || values.after !== undefined) && !['list', 'inspect'].includes(command)) throw new QueueLiteError('--limit and --after are supported only by list and inspect.');
    const limit = number(values.limit, 'limit', 1);
    const after = number(values.after, 'after', 0);
    if (values.status !== undefined && !['pending', 'active', 'completed', 'failed', 'cancelled', 'delayed'].includes(values.status)) throw new QueueLiteError('Invalid status. Use pending, active, completed, failed, cancelled or delayed.');
    const database = values.db ?? './queuelite.db';
    if (!database.trim() || database.includes('\0') || database === ':memory:') throw new QueueLiteError('--db must identify an existing SQLite file.');
    usage = false;
    if (command === 'doctor') {
      const result = diagnose({ database }); print(result, json); process.exitCode = result.ok ? 0 : 1; return;
    }
    let queue = createQueue({ database, readOnly: true });
    try {
      const options = { ...(limit === undefined ? {} : { limit }), ...(after === undefined ? {} : { after }) };
      if (command === 'stats') { print(queue.getStats(), json); return; }
      if (command === 'list') {
        print(queue.listJobs({ ...options, ...(values.status === undefined ? {} : { status: values.status as NonNullable<ListOptions['status']> }) }), json); return;
      }
      const job = queue.getJobSummary(id!);
      if (!job) { print({ error: 'Job not found. Check the job ID and database.', retryable: false }, json); process.exitCode = 3; return; }
      if (command === 'inspect') {
        const safeAttempts = (failuresOnly: boolean) => {
          const page = failuresOnly ? queue.getFailureHistory(id!, options) : queue.getAttempts(id!, options);
          return { ...page, items: page.items.map((attempt) => ({ ...attempt,
            error: attempt.error === null ? null : { kind: attempt.error.kind, attempt: attempt.error.attempt, at: attempt.error.at } })) };
        };
        print({ job: metadata(job), attempts: safeAttempts(false), failures: safeAttempts(true) }, json);
      } else {
        await queue.close();
        queue = createQueue({ database, fileMustExist: true });
        print(metadata(command === 'retry' ? queue.retryJob(id!) : queue.cancelJob(id!)), json);
      }
    } finally { await queue.close(); }
  } catch (cause) {
    const message = cause instanceof QueueLiteError ? cause.message : usage ? 'Invalid arguments. Use --help for supported commands and options.' : 'Operation failed. Run queuelite doctor and check storage permissions.';
    print({ error: message, retryable: cause instanceof QueueLiteError && cause.retryable }, json);
    process.exitCode = usage ? 2 : 1;
  }
}

await main();
