import type Database from 'better-sqlite3';

export const SCHEMA_VERSION = 3;
export function checkJobsSchema(db: Database.Database): void {
  db.prepare(`SELECT seq,id,name,payload,status,created_at,updated_at,attempts,run_at,started_at,finished_at,error,
    max_attempts,backoff_type,backoff_delay,backoff_jitter,priority,idempotency_key,error_history,lease_token,lease_expires_at FROM jobs LIMIT 0`).all();
}
export function checkSchema(db: Database.Database): void {
  checkJobsSchema(db);
  db.prepare('SELECT seq,job_id,attempt,started_at,finished_at,outcome,is_retry,error FROM job_attempts LIMIT 0').all();
}
