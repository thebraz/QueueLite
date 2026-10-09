export class QueueLiteError extends Error {
  override name = 'QueueLiteError';
  readonly code?: string;
  readonly retryable: boolean;
  constructor(message: string, options?: ErrorOptions & { code?: string; retryable?: boolean }) {
    super(message, options);
    if (options?.code !== undefined) this.code = options.code;
    this.retryable = options?.retryable ?? false;
  }
}

export function storageError(cause: unknown, operation: string): QueueLiteError {
  if (cause instanceof QueueLiteError) return cause;
  const code = (cause as { code?: unknown } | null)?.code;
  const busy = typeof code === 'string' && /^(SQLITE_BUSY|SQLITE_LOCKED)/.test(code);
  const reason = busy ? 'Storage is busy; retry with bounded asynchronous backoff.'
    : code === 'SQLITE_READONLY' ? 'Storage is read-only; open a writable queue to make changes.'
    : code === 'SQLITE_FULL' ? 'Storage is full; free disk space without deleting queue data, then retry.'
    : typeof code === 'string' && code.startsWith('SQLITE_IOERR') ? 'Storage I/O failed; check disk health and permissions before retrying.'
    : code === 'SQLITE_CORRUPT' || code === 'SQLITE_NOTADB' ? 'Storage is damaged or is not SQLite; preserve it and restore a verified backup.'
    : 'Check the database path, permissions, schema compatibility and available disk space; run queuelite doctor.';
  return new QueueLiteError(`${operation}. ${reason}`, { cause, retryable: busy,
    ...(typeof code === 'string' && /^SQLITE_[A-Z_]+$/.test(code) ? { code } : {}) });
}
