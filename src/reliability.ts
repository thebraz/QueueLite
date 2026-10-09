import type { BackoffOptions, JobError } from './types.js';

export function retryDelay(backoff: BackoffOptions, attempt: number, random = Math.random): number {
  const base = Math.min(Number.MAX_SAFE_INTEGER, backoff.delay === 0 ? 0 : backoff.delay * (backoff.type === 'exponential' ? 2 ** (attempt - 1) : 1));
  return Math.floor(base * (1 - (backoff.jitter ?? 0) * random()));
}

function redact(value: string): string {
  return value
    .replace(/((?:bearer|basic)\s+)\S+/gi, '$1[REDACTED]')
    .replace(/(["']?(?:password|passwd|secret|token|api[_-]?key|authorization)["']?\s*[=:]\s*)(?:"[^"]*"|'[^']*'|[^\s,;]+)/gi, '$1[REDACTED]')
    .replace(/(https?:\/\/)[^\s/@]+:[^\s/@]+@/gi, '$1[REDACTED]@')
    .replace(/\bsk-[a-zA-Z0-9_-]{12,}/g, '[REDACTED]')
    .slice(0, 2048);
}

export function serializeError(cause: unknown, attempt: number, at: number, kind: JobError['kind'] = 'handler'): JobError {
  let name = 'Error';
  let message = 'Handler threw an unprintable value.';
  let code: string | undefined;
  try {
    if (cause instanceof Error) {
      const ownMessage = Object.getOwnPropertyDescriptor(cause, 'message')?.value as unknown;
      const ownName = Object.getOwnPropertyDescriptor(cause, 'name')?.value as unknown;
      const prototypeName = Object.getOwnPropertyDescriptor(Object.getPrototypeOf(cause) as object, 'name')?.value as unknown;
      const ownCode = Object.getOwnPropertyDescriptor(cause, 'code')?.value as unknown;
      if (typeof ownMessage === 'string') message = ownMessage;
      if (typeof ownName === 'string') name = ownName;
      else if (typeof prototypeName === 'string') name = prototypeName;
      if (typeof ownCode === 'string') code = redact(ownCode).slice(0, 128);
    } else if (cause === null || ['string', 'number', 'boolean', 'undefined', 'bigint'].includes(typeof cause)) {
      message = String(cause);
    }
  } catch { /* Thrown proxies and accessors must not break failure recording. */ }
  return { attempt, at, kind, name: redact(name).slice(0, 128), message: redact(message), ...(code === undefined ? {} : { code }) };
}

export function isBusy(cause: unknown): boolean {
  const code = (cause as { code?: unknown } | null)?.code;
  return typeof code === 'string' && (code.startsWith('SQLITE_BUSY') || code.startsWith('SQLITE_LOCKED'));
}
