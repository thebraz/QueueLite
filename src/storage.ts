import Database from 'better-sqlite3';
import { randomUUID } from 'node:crypto';
import { QueueLiteError, storageError } from './errors.js';
import { addOptions } from './validation.js';
import { retryDelay, serializeError } from './reliability.js';
import { checkJobsSchema, checkSchema, SCHEMA_VERSION } from './schema.js';
import type { AddOptions, Job, JobError, JobStatus, QueueOptions, LifecycleEvent, Page, JobSummary,
  JobAttempt, QueueStats, ListOptions } from './types.js';

interface Row {
  id: string; name: string; payload: string; status: JobStatus;
  created_at: number; updated_at: number; attempts: number; run_at: number;
  started_at: number | null; finished_at: number | null; error: string | null;
  max_attempts: number; backoff_type: 'fixed' | 'exponential'; backoff_delay: number; backoff_jitter: number;
  priority: number; idempotency_key: string | null; error_history: string;
  lease_token: string | null; lease_expires_at: number | null;
}
export interface Claim { job: Job; token: string }
function decode(row: Row): Job {
  return { ...summarize(row), data: JSON.parse(row.payload) as Job['data'], error: row.error,
    idempotencyKey: row.idempotency_key, errorHistory: JSON.parse(row.error_history) as JobError[] };
}
function summarize(row: Row): JobSummary {
  return { id: row.id, name: row.name, status: row.status,
    createdAt: row.created_at, updatedAt: row.updated_at, attempts: row.attempts,
    runAt: row.run_at, startedAt: row.started_at, finishedAt: row.finished_at,
    maxAttempts: row.max_attempts, backoff: { type: row.backoff_type, delay: row.backoff_delay, jitter: row.backoff_jitter },
    priority: row.priority, leaseExpiresAt: row.lease_expires_at };
}
function page<R extends { seq: number }, T>(rows: R[], limit: number, map: (row: R) => T): Page<T> {
  const selected = rows.slice(0, limit);
  return { items: selected.map(map), nextCursor: rows.length > limit ? selected.at(-1)!.seq : null };
}

export class Storage {
  private readonly db: Database.Database;
  constructor(database: string, private readonly options: Omit<QueueOptions, 'database'> = {}) {
    try { this.db = new Database(database, { timeout: 5000, readonly: options.readOnly ?? false,
      fileMustExist: options.readOnly || options.fileMustExist || false }); }
    catch (cause) { throw storageError(cause, 'Could not open queue database'); }
    try {
      const initialVersion = this.db.pragma('user_version', { simple: true });
      if (![0, 1, 2, SCHEMA_VERSION].includes(initialVersion as number)) throw new QueueLiteError('Could not initialize queue database: unsupported database schema version; use a compatible SDK.');
      if (options.readOnly) {
        if (initialVersion !== SCHEMA_VERSION) throw new QueueLiteError('Could not initialize read-only queue: schema upgrade required; back up the database, stop old workers, and open it with the current writable SDK.');
        checkSchema(this.db);
        this.db.pragma('busy_timeout = 0');
        return;
      }
      if (initialVersion === 2) checkJobsSchema(this.db);
      if (initialVersion === SCHEMA_VERSION) checkSchema(this.db);
      this.db.pragma('journal_mode = WAL');
      this.db.pragma('synchronous = FULL');
      this.db.transaction(() => {
        const version = this.db.pragma('user_version', { simple: true });
        if (version === SCHEMA_VERSION) return;
        if (version === 2) { this.migrateObservability(); return; }
        if (version !== 0 && version !== 1) throw new QueueLiteError('Unsupported database schema version; use a compatible SDK.');
        if (version === 1) this.db.exec('DROP INDEX jobs_pending; ALTER TABLE jobs RENAME TO jobs_v1;');
        this.db.exec(`
          CREATE TABLE jobs (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            id TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL CHECK(length(name) BETWEEN 1 AND 255),
            payload TEXT NOT NULL CHECK(json_valid(payload)),
            status TEXT NOT NULL CHECK(status IN ('pending', 'active', 'completed', 'failed', 'cancelled')),
            created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
            run_at INTEGER NOT NULL, started_at INTEGER, finished_at INTEGER, error TEXT,
            max_attempts INTEGER NOT NULL DEFAULT 1 CHECK(max_attempts BETWEEN 1 AND 1000),
            backoff_type TEXT NOT NULL DEFAULT 'fixed' CHECK(backoff_type IN ('fixed', 'exponential')),
            backoff_delay INTEGER NOT NULL DEFAULT 0 CHECK(backoff_delay >= 0),
            backoff_jitter REAL NOT NULL DEFAULT 0 CHECK(backoff_jitter BETWEEN 0 AND 1),
            priority INTEGER NOT NULL DEFAULT 0, idempotency_key TEXT,
            error_history TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(error_history)),
            lease_token TEXT, lease_expires_at INTEGER,
            CHECK((status = 'active' AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
              OR (status != 'active' AND lease_token IS NULL AND lease_expires_at IS NULL)),
            CHECK((status = 'pending' AND started_at IS NULL AND finished_at IS NULL AND error IS NULL)
              OR (status = 'active' AND started_at IS NOT NULL AND finished_at IS NULL AND error IS NULL)
              OR (status = 'completed' AND started_at IS NOT NULL AND finished_at IS NOT NULL AND error IS NULL)
              OR (status = 'failed' AND started_at IS NOT NULL AND finished_at IS NOT NULL AND error IS NOT NULL)
              OR (status = 'cancelled' AND started_at IS NULL AND finished_at IS NOT NULL AND error IS NULL))
          );
          CREATE INDEX jobs_pending ON jobs(priority DESC, seq, run_at) WHERE status = 'pending';
          CREATE INDEX jobs_expired ON jobs(lease_expires_at) WHERE status = 'active';
          CREATE INDEX jobs_failed ON jobs(seq) WHERE status = 'failed';
          CREATE UNIQUE INDEX jobs_idempotency ON jobs(name, idempotency_key) WHERE idempotency_key IS NOT NULL;
        `);
        if (version === 1) this.db.exec(`
          INSERT INTO jobs (seq, id, name, payload, status, created_at, updated_at, attempts, run_at,
            started_at, finished_at, error, max_attempts, lease_token, lease_expires_at)
          SELECT rowid, id, name, payload, status, created_at, updated_at, attempts, run_at,
            started_at, finished_at, error, min(1000, max(1, attempts + (status = 'active'))),
            CASE WHEN status = 'active' THEN 'legacy:' || id END,
            CASE WHEN status = 'active' THEN 0 END FROM jobs_v1 ORDER BY rowid;
          DROP TABLE jobs_v1;
        `);
        this.migrateObservability();
      }).immediate();
      checkSchema(this.db);
      // Runtime contention must yield to timers/heartbeats, rather than block Node for five seconds.
      this.db.pragma('busy_timeout = 0');
    } catch (cause) {
      this.db.close();
      throw storageError(cause, 'Could not initialize queue database');
    }
  }
  private migrateObservability(): void {
    this.db.exec(`
      CREATE INDEX jobs_status_seq ON jobs(status, seq);
      CREATE INDEX jobs_schedule ON jobs(run_at, seq) WHERE status = 'pending';
      CREATE TABLE job_attempts (
        seq INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL REFERENCES jobs(id),
        attempt INTEGER NOT NULL, started_at INTEGER, finished_at INTEGER,
        outcome TEXT NOT NULL CHECK(outcome IN ('active','completed','failed','recovered')),
        is_retry INTEGER NOT NULL CHECK(is_retry IN (0,1)), error TEXT CHECK(error IS NULL OR json_valid(error))
      );
      CREATE INDEX attempts_job ON job_attempts(job_id, seq);
      CREATE INDEX attempts_failures ON job_attempts(job_id, seq) WHERE error IS NOT NULL;
      CREATE UNIQUE INDEX attempts_active ON job_attempts(job_id) WHERE outcome = 'active';
      INSERT INTO job_attempts (job_id, attempt, finished_at, outcome, is_retry, error)
        SELECT jobs.id, json_extract(h.value, '$.attempt'), json_extract(h.value, '$.at'),
          CASE WHEN json_extract(h.value, '$.kind') = 'lease-expired' THEN 'recovered' ELSE 'failed' END,
          CASE WHEN CAST(h.key AS INTEGER) > 0 OR json_extract(h.value, '$.attempt') > 1 THEN 1 ELSE 0 END, h.value
        FROM jobs, json_each(jobs.error_history) h ORDER BY jobs.seq, CAST(h.key AS INTEGER);
      INSERT INTO job_attempts (job_id, attempt, started_at, finished_at, outcome, is_retry, error)
        SELECT id, attempts, started_at, finished_at,
          CASE WHEN status = 'active' THEN 'active' WHEN status = 'completed' THEN 'completed' ELSE 'failed' END,
          CASE WHEN attempts > 1 OR json_array_length(error_history) > 0 THEN 1 ELSE 0 END,
          CASE WHEN status = 'failed' THEN json_object('attempt',attempts,'at',finished_at,'kind','handler','name','LegacyError','message','Legacy failure; inspect the preserved job error.') END
        FROM jobs WHERE status IN ('active','completed') OR (status = 'failed' AND json_array_length(error_history) = 0);
      PRAGMA user_version = 3;
    `);
  }
  private publish(events: LifecycleEvent[]): void {
    for (const event of events) {
      const snapshot = Object.freeze({ ...event });
      for (const hook of [this.options.onEvent, this.options.logger && ((value: LifecycleEvent) => this.options.logger!.info(value))]) {
        try {
          const result: unknown = hook?.(snapshot);
          if (result && typeof result === 'object' && 'then' in result) void Promise.resolve(result).catch(() => {});
        } catch { /* Observers cannot roll back or interrupt committed queue transitions. */ }
      }
    }
  }
  private transaction<T>(action: (events: LifecycleEvent[]) => T): T {
    const events: LifecycleEvent[] = [];
    const result = this.db.transaction(() => action(events)).immediate();
    this.publish(events);
    return result;
  }
  private event(type: LifecycleEvent['type'], row: Row, at: number, status: JobStatus = row.status): LifecycleEvent {
    return { type, jobId: row.id, at, attempt: row.attempts, status };
  }
  add(name: string, payload: string, options: AddOptions = {}): Job {
    const config = addOptions(options);
    return this.transaction((events) => {
      const id = randomUUID();
      const now = Date.now();
      const inserted = this.db.prepare(`INSERT INTO jobs (id, name, payload, status, created_at, updated_at, run_at,
        max_attempts, backoff_type, backoff_delay, backoff_jitter, priority, idempotency_key)
        VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(name, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING`)
        .run(id, name, payload, now, now, config.runAt, config.attempts, config.backoff.type,
          config.backoff.delay, config.backoff.jitter, config.priority, config.idempotencyKey);
      const row = this.db.prepare('SELECT * FROM jobs WHERE id = ? OR (name = ? AND idempotency_key = ?)')
        .get(id, name, config.idempotencyKey) as Row;
      if (inserted.changes) events.push(this.event('enqueued', row, now));
      return decode(row);
    });
  }
  get(id: string): Job | undefined {
    const row = this.db.prepare('SELECT * FROM jobs WHERE id = ?').get(id) as Row | undefined;
    return row && decode(row);
  }
  summary(id: string): JobSummary | undefined {
    const row = this.db.prepare(`SELECT id, name, status, created_at, updated_at, attempts, run_at,
      started_at, finished_at, max_attempts, backoff_type, backoff_delay, backoff_jitter, priority, lease_expires_at
      FROM jobs WHERE id = ?`).get(id) as Row | undefined;
    return row && summarize(row);
  }
  list(options: Required<Pick<ListOptions, 'limit' | 'after'>> & Pick<ListOptions, 'status'>): Page<JobSummary> {
    const values: (string | number)[] = [options.after];
    let filter = '';
    if (options.status === 'delayed') { filter = "AND status = 'pending' AND run_at > ?"; values.push(Date.now()); }
    else if (options.status !== undefined) { filter = 'AND status = ?'; values.push(options.status); }
    values.push(options.limit + 1);
    const rows = this.db.prepare(`SELECT seq, id, name, status, created_at, updated_at, attempts, run_at,
      started_at, finished_at, max_attempts, backoff_type, backoff_delay, backoff_jitter, priority, lease_expires_at
      FROM jobs WHERE seq > ? ${filter} ORDER BY seq LIMIT ?`).all(...values) as (Row & { seq: number })[];
    return page(rows, options.limit, summarize);
  }
  attempts(id: string, limit: number, after: number, failuresOnly = false): Page<JobAttempt> {
    interface AttemptRow {
      seq: number; job_id: string; attempt: number; started_at: number | null; finished_at: number | null;
      outcome: JobAttempt['outcome']; is_retry: number; error: string | null;
    }
    const rows = this.db.prepare(`SELECT * FROM job_attempts WHERE job_id = ? AND seq > ?
      ${failuresOnly ? 'AND error IS NOT NULL' : ''} ORDER BY seq LIMIT ?`).all(id, after, limit + 1) as AttemptRow[];
    return page(rows, limit, (row) => ({ id: row.seq, jobId: row.job_id, attempt: row.attempt,
      startedAt: row.started_at, finishedAt: row.finished_at, outcome: row.outcome, retry: row.is_retry === 1,
      error: row.error === null ? null : JSON.parse(row.error) as JobError }));
  }
  stats(): QueueStats {
    return this.db.transaction(() => {
      const now = Date.now();
      const counts = this.db.prepare(`SELECT count(*) AS total,
        coalesce(sum(status = 'pending' AND run_at <= ?), 0) AS pending,
        coalesce(sum(status = 'pending' AND run_at > ?), 0) AS delayed,
        coalesce(sum(status = 'active'), 0) AS active, coalesce(sum(status = 'completed'), 0) AS completed,
        coalesce(sum(status = 'failed'), 0) AS failed, coalesce(sum(status = 'cancelled'), 0) AS cancelled FROM jobs`)
        .get(now, now) as Omit<QueueStats, 'retryAttempts' | 'outcomes' | 'averageDurationMs'>;
      const history = this.db.prepare(`SELECT coalesce(sum(is_retry), 0) AS retryAttempts,
        coalesce(sum(outcome = 'completed'), 0) AS completed, coalesce(sum(outcome = 'failed'), 0) AS failed,
        coalesce(sum(outcome = 'recovered'), 0) AS recovered,
        avg(CASE WHEN finished_at IS NOT NULL AND started_at IS NOT NULL THEN max(0, finished_at - started_at) END) AS averageDurationMs
        FROM job_attempts`).get() as { retryAttempts: number; completed: number; failed: number; recovered: number; averageDurationMs: number | null };
      return { ...counts, retryAttempts: history.retryAttempts, averageDurationMs: history.averageDurationMs,
        outcomes: { completed: history.completed, failed: history.failed, recovered: history.recovered } };
    }).deferred();
  }
  failed(limit: number): Job[] {
    return (this.db.prepare("SELECT * FROM jobs WHERE status = 'failed' ORDER BY seq LIMIT ?").all(limit) as Row[]).map(decode);
  }
  retry(id: string): Job {
    const now = Date.now();
    const row = this.db.prepare(`UPDATE jobs SET status = 'pending', attempts = 0, run_at = ?, updated_at = ?,
      started_at = NULL, finished_at = NULL, error = NULL WHERE id = ? AND status = 'failed' RETURNING *`)
      .get(now, now, id) as Row | undefined;
    if (!row) throw new QueueLiteError('Only failed jobs may be manually retried. Inspect the job ID and status; fix its handler before retrying.');
    this.publish([this.event('retried', row, now)]);
    return decode(row);
  }
  cancel(id: string): Job {
    const now = Date.now();
    const row = this.db.prepare(`UPDATE jobs SET status = 'cancelled', finished_at = ?, updated_at = ?
      WHERE id = ? AND status = 'pending' RETURNING *`).get(now, now, id) as Row | undefined;
    if (!row) throw new QueueLiteError('Only pending jobs may be cancelled. Inspect the job ID and status; active claims must finish or recover under their lease.');
    this.publish([this.event('cancelled', row, now)]);
    return decode(row);
  }
  private fail(row: Row, error: JobError, now: number, events: LifecycleEvent[], permanent = false): void {
    const retry = !permanent && row.attempts < row.max_attempts;
    const next = Math.min(Number.MAX_SAFE_INTEGER, now + retryDelay({ type: row.backoff_type,
      delay: row.backoff_delay, jitter: row.backoff_jitter }, row.attempts));
    this.db.prepare(`UPDATE jobs SET status = ?, run_at = ?, updated_at = ?, started_at = ?, finished_at = ?,
      error = ?, lease_token = NULL, lease_expires_at = NULL, error_history = json_insert(error_history, '$[#]', json(?))
      WHERE id = ? AND status = 'active' AND lease_token = ?`).run(retry ? 'pending' : 'failed', retry ? next : row.run_at,
      now, retry ? null : row.started_at, retry ? null : now, retry ? null : `${error.name}: ${error.message}`, JSON.stringify(error), row.id, row.lease_token);
    this.db.prepare(`UPDATE job_attempts SET finished_at = ?, outcome = ?, error = ? WHERE job_id = ? AND outcome = 'active'`)
      .run(now, error.kind === 'lease-expired' ? 'recovered' : 'failed', JSON.stringify(error), row.id);
    const status = retry ? 'pending' : 'failed';
    events.push(this.event('failed', row, now, status));
    if (error.kind === 'lease-expired') events.push(this.event('recovered', row, now, status));
    if (retry) events.push(this.event('retried', row, now, status));
  }
  claim(leaseDurationMs = 30000): Claim | undefined {
    return this.transaction((events) => {
      const now = Date.now();
      const expired = this.db.prepare(`SELECT * FROM jobs WHERE status = 'active' AND lease_expires_at <= ?
        ORDER BY lease_expires_at, seq LIMIT 100`).all(now) as Row[];
      for (const row of expired) this.fail(row, serializeError(new Error('Worker lease expired.'), row.attempts, now, 'lease-expired'), now, events);
      const token = randomUUID();
      const row = this.db.prepare(`UPDATE jobs SET status = 'active', attempts = attempts + 1,
        started_at = ?, updated_at = ?, lease_token = ?, lease_expires_at = ? WHERE id = (
          SELECT id FROM jobs WHERE status = 'pending' AND run_at <= ? AND attempts < max_attempts
          ORDER BY priority DESC, seq LIMIT 1
        ) AND status = 'pending' RETURNING *`).get(now, now, token, now + leaseDurationMs, now) as Row | undefined;
      if (!row) return undefined;
      this.db.prepare(`INSERT INTO job_attempts (job_id, attempt, started_at, outcome, is_retry)
        VALUES (?, ?, ?, 'active', EXISTS(SELECT 1 FROM job_attempts WHERE job_id = ?))`).run(row.id, row.attempts, now, row.id);
      events.push(this.event('started', row, now));
      return { job: decode(row), token };
    });
  }
  hasEligible(): boolean {
    const now = Date.now();
    return this.db.prepare(`SELECT 1 FROM jobs WHERE (status = 'pending' AND run_at <= ? AND attempts < max_attempts)
      OR (status = 'active' AND lease_expires_at <= ?) LIMIT 1`).get(now, now) !== undefined;
  }
  renew(id: string, token: string, duration: number): boolean {
    return this.db.transaction(() => {
      const now = Date.now();
      return this.db.prepare(`UPDATE jobs SET lease_expires_at = ?, updated_at = ?
        WHERE id = ? AND status = 'active' AND lease_token = ? AND lease_expires_at > ?`)
        .run(now + duration, now, id, token, now).changes === 1;
    }).immediate();
  }
  finish(id: string, token: string, error: JobError | null, permanent = false): boolean {
    return this.transaction((events) => {
      const now = Date.now();
      const row = this.db.prepare(`SELECT * FROM jobs WHERE id = ? AND status = 'active'
        AND lease_token = ? AND lease_expires_at > ?`).get(id, token, now) as Row | undefined;
      if (!row) return false;
      if (error !== null) this.fail(row, error, now, events, permanent);
      else {
        this.db.prepare(`UPDATE jobs SET status = 'completed', finished_at = ?, updated_at = ?,
        lease_token = NULL, lease_expires_at = NULL WHERE id = ? AND status = 'active' AND lease_token = ?`)
        .run(now, now, id, token);
        this.db.prepare(`UPDATE job_attempts SET finished_at = ?, outcome = 'completed' WHERE job_id = ? AND outcome = 'active'`).run(now, id);
        events.push({ ...this.event('completed', row, now, 'completed'), durationMs: Math.max(0, now - row.started_at!) });
      }
      return true;
    });
  }
  close(): void { this.db.close(); }
}
