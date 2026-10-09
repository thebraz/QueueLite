import { QueueLiteError } from './errors.js';
import type { AddOptions, BackoffOptions, PageOptions, QueueOptions } from './types.js';

export function queueOptions(options: QueueOptions): void {
  validateObject(options, 'Queue options');
  if (typeof options.database !== 'string' || !options.database.trim() || options.database.includes('\0')) {
    throw new QueueLiteError('database must be a non-empty SQLite file path or :memory:.');
  }
  for (const key of ['readOnly', 'fileMustExist'] as const) {
    if (options[key] !== undefined && typeof options[key] !== 'boolean') throw new QueueLiteError(`${key} must be a boolean.`);
  }
  if (options.onEvent !== undefined && typeof options.onEvent !== 'function') throw new QueueLiteError('onEvent must be a function.');
  if (options.logger !== undefined && (!options.logger || typeof options.logger.info !== 'function')) throw new QueueLiteError('logger must provide an info(event) function.');
  if (options.readOnly && options.database === ':memory:') throw new QueueLiteError('readOnly requires an existing database file.');
}

export function pagination(options: PageOptions): Required<PageOptions> {
  validateObject(options, 'Pagination options');
  return { limit: integer(options.limit === undefined ? 100 : options.limit, 'limit', 1, 1000),
    after: integer(options.after === undefined ? 0 : options.after, 'after') };
}

export function validateObject(value: unknown, label: string): void {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new QueueLiteError(`${label} must be an object.`);
}
export function integer(value: number, label: string, minimum = 0, maximum = Number.MAX_SAFE_INTEGER): number {
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new QueueLiteError(`${label} must be an integer between ${minimum} and ${maximum}.`);
  }
  return value;
}
export function addOptions(options: AddOptions): Required<Omit<AddOptions, 'delay' | 'idempotencyKey'>> & { idempotencyKey: string | null } {
  validateObject(options, 'Job options');
  if (options.delay !== undefined && options.runAt !== undefined) throw new QueueLiteError('Use either delay or runAt.');
  const delay = integer(options.delay === undefined ? 0 : options.delay, 'delay');
  const runAt = integer(options.runAt === undefined ? Date.now() + delay : options.runAt, 'runAt');
  const attempts = integer(options.attempts === undefined ? 1 : options.attempts, 'attempts', 1, 1000);
  const priority = integer(options.priority === undefined ? 0 : options.priority, 'priority', -2_147_483_648, 2_147_483_647);
  const backoff: BackoffOptions = options.backoff === undefined ? { type: 'fixed', delay: 0 } : options.backoff;
  validateObject(backoff, 'backoff');
  if (backoff.type !== 'fixed' && backoff.type !== 'exponential') throw new QueueLiteError('Invalid backoff type.');
  integer(backoff.delay, 'backoff.delay');
  const jitter = backoff.jitter === undefined ? 0 : backoff.jitter;
  if (!Number.isFinite(jitter) || jitter < 0 || jitter > 1) throw new QueueLiteError('backoff.jitter must be between 0 and 1.');
  const key = options.idempotencyKey;
  if (key !== undefined && (typeof key !== 'string' || !key.trim() || key.length > 255 || key.includes('\0'))) {
    throw new QueueLiteError('idempotencyKey must be a non-empty string of at most 255 characters without null bytes.');
  }
  return { runAt, attempts, priority, backoff: { type: backoff.type, delay: backoff.delay, jitter }, idempotencyKey: key ?? null };
}

export function validateName(name: unknown): asserts name is string {
  if (typeof name !== 'string' || name.trim() !== name || name.length === 0 || name.length > 255 || name.includes('\0')) {
    throw new QueueLiteError('Job name must be a non-empty, trimmed string of at most 255 characters without null bytes.');
  }
}

export function serializePayload(value: unknown): string {
  const ancestors = new Set<object>();
  function visit(item: unknown): void {
    if (item === null || typeof item === 'string' || typeof item === 'boolean') return;
    if (typeof item === 'number' && Number.isFinite(item)) return;
    if (typeof item !== 'object' || item === null || ancestors.has(item)) {
      throw new QueueLiteError('Payload must contain only finite, acyclic JSON data.');
    }
    const array = Array.isArray(item);
    const prototype = Object.getPrototypeOf(item);
    if (prototype !== (array ? Array.prototype : Object.prototype) && prototype !== null) {
      throw new QueueLiteError('Payload objects and arrays must have plain JSON prototypes.');
    }
    ancestors.add(item);
    const keys = Reflect.ownKeys(item);
    if (array && keys.length !== item.length + 1) throw new QueueLiteError('Payload arrays must be dense and have no extra properties.');
    for (const key of keys) {
      if (array && key === 'length') continue;
      if (array && (typeof key !== 'string' || String(Number(key)) !== key || !Number.isInteger(Number(key)) || Number(key) < 0 || Number(key) >= item.length)) {
        throw new QueueLiteError('Payload arrays must be dense and have no extra properties.');
      }
      const descriptor = Object.getOwnPropertyDescriptor(item, key);
      if (typeof key !== 'string' || !descriptor?.enumerable || !('value' in descriptor)) {
        throw new QueueLiteError('Payload cannot contain symbols, accessors, or hidden properties.');
      }
      visit(descriptor.value);
    }
    ancestors.delete(item);
  }
  try {
    visit(value);
    return JSON.stringify(value);
  } catch (cause) {
    if (cause instanceof QueueLiteError) throw cause;
    throw new QueueLiteError('Payload could not be serialized.', { cause });
  }
}
