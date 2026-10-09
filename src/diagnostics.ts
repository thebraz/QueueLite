import Database from 'better-sqlite3';
import { accessSync, constants } from 'node:fs';
import { dirname } from 'node:path';
import { checkSchema, SCHEMA_VERSION } from './schema.js';
import { queueOptions } from './validation.js';
import type { DiagnosticCheck, DiagnosticReport, QueueOptions } from './types.js';

/** Reads existing storage only. Does not initialize, migrate, recover or repair jobs. */
export function diagnose(options: QueueOptions): DiagnosticReport {
  const checks: DiagnosticCheck[] = [];
  let schemaVersion: number | null = null;
  const report = (): DiagnosticReport => ({ ok: checks.every((check) => check.status === 'ok'), schemaVersion, checks });
  try { queueOptions(options); }
  catch { checks.push({ name: 'configuration', status: 'error', message: 'Supply a valid database file path and queue options.' }); return report(); }
  if (options.database === ':memory:') {
    checks.push({ name: 'configuration', status: 'error', message: 'Diagnostics require an existing database file; :memory: would inspect a different database.' });
    return report();
  }
  checks.push({ name: 'configuration', status: 'ok', message: 'Queue options are valid.' });
  try {
    accessSync(options.database, constants.W_OK);
    accessSync(dirname(options.database), constants.W_OK);
    checks.push({ name: 'writable-storage', status: 'ok', message: 'File and parent directory allow writes according to the OS permission check.' });
  } catch { checks.push({ name: 'writable-storage', status: 'warning', message: 'File or parent directory is missing or not writable; check the path, permissions and read-only deployment settings.' }); }
  let db: Database.Database;
  try { db = new Database(options.database, { readonly: true, fileMustExist: true, timeout: 1000 }); }
  catch { checks.push({ name: 'database', status: 'error', message: 'Cannot open existing storage. Check the path and read permissions; no database was created.' }); return report(); }
  try {
    db.transaction(() => {
      const integrity = db.pragma('quick_check(1)') as { quick_check: string }[];
      if (integrity.length !== 1 || integrity[0]?.quick_check !== 'ok') throw new Error('integrity');
      checks.push({ name: 'integrity', status: 'ok', message: 'SQLite basic integrity check passed.' });
      schemaVersion = db.pragma('user_version', { simple: true }) as number;
      if (schemaVersion !== SCHEMA_VERSION) {
        checks.push({ name: 'schema', status: 'error', message: schemaVersion === 1 || schemaVersion === 2
          ? 'Schema upgrade required. Back up storage, stop old workers, then open it with the current writable SDK; doctor does not migrate.'
          : 'Incompatible or uninitialized schema. Use the matching SDK or restore a verified backup; do not overwrite this file.' });
        return;
      }
      // Prepare all engine columns too: a user_version alone does not establish compatibility.
      checkSchema(db);
      checks.push({ name: 'schema', status: 'ok', message: `Schema version ${SCHEMA_VERSION} and required columns are present.` });
      const inconsistent = db.prepare(`SELECT count(*) AS count FROM jobs j WHERE
        status NOT IN ('pending','active','completed','failed','cancelled') OR attempts < 0 OR max_attempts NOT BETWEEN 1 AND 1000
        OR attempts > max_attempts OR (status = 'pending' AND attempts >= max_attempts)
        OR NOT json_valid(payload) OR CASE WHEN json_valid(error_history) THEN json_type(error_history) != 'array' ELSE 1 END
        OR (status = 'active' AND (lease_token IS NULL OR lease_expires_at IS NULL OR started_at IS NULL OR finished_at IS NOT NULL OR error IS NOT NULL))
        OR (status != 'active' AND (lease_token IS NOT NULL OR lease_expires_at IS NOT NULL))
        OR (status = 'pending' AND (started_at IS NOT NULL OR finished_at IS NOT NULL OR error IS NOT NULL))
        OR (status = 'completed' AND (started_at IS NULL OR finished_at IS NULL OR error IS NOT NULL))
        OR (status = 'failed' AND (started_at IS NULL OR finished_at IS NULL OR error IS NULL))
        OR (status = 'cancelled' AND (started_at IS NOT NULL OR finished_at IS NULL OR error IS NOT NULL))
        OR (status = 'active' AND NOT EXISTS (SELECT 1 FROM job_attempts a WHERE a.job_id = j.id AND a.outcome = 'active' AND a.attempt = j.attempts))`)
        .get() as { count: number };
      const brokenAttempts = db.prepare(`SELECT count(*) AS count FROM job_attempts a LEFT JOIN jobs j ON j.id = a.job_id
        WHERE j.id IS NULL OR (a.outcome = 'active' AND (j.status != 'active' OR a.finished_at IS NOT NULL))
        OR (a.outcome != 'active' AND a.finished_at IS NULL)`).get() as { count: number };
      checks.push({ name: 'state', status: inconsistent.count + brokenAttempts.count ? 'error' : 'ok',
        message: inconsistent.count + brokenAttempts.count ? 'Inconsistent queue state detected. Stop workers, preserve a backup and investigate before recovery.' : 'Job states and active attempt records are consistent.' });
      const abandoned = db.prepare("SELECT count(*) AS count FROM jobs WHERE status = 'active' AND lease_expires_at <= ?").get(Date.now()) as { count: number };
      checks.push({ name: 'leases', status: abandoned.count ? 'warning' : 'ok', message: abandoned.count
        ? `${abandoned.count} expired claims. Start a worker with the required handlers to recover under the existing retry policy; exhausted jobs become failed. Effects may repeat.`
        : 'No expired active claims. A valid lease alone does not prove a worker is alive.' });
    }).deferred();
  } catch {
    checks.push({ name: 'storage', status: 'error', message: 'Could not inspect storage integrity or schema. Check locks and compatibility; preserve damaged storage and restore a verified backup.' });
  } finally { db.close(); }
  return report();
}
