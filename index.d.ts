// High-level typed surface for the honker Node binding. The low-level
// napi-rs types still exist (see ./native.js) — this file mirrors the
// Rust / Python / Ruby / Elixir bindings on top of them.

export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

// ----- options -----

export interface QueueOpts {
  /** Visibility timeout (seconds) for claimed jobs. Default: 300. */
  visibilityTimeoutS?: number;
  /** Max delivery attempts before a job is moved to `_honker_dead`. Default: 3. */
  maxAttempts?: number;
}

export interface EnqueueOpts {
  /** Seconds of delay before the job becomes runnable. */
  delay?: number;
  /** Absolute unix timestamp to run at (instead of `delay`). */
  runAt?: number;
  /** Higher = earlier. Default 0. */
  priority?: number;
  /** Unix timestamp after which the job should expire (before running). */
  expires?: number;
  /** Enqueue on an open transaction — atomic with other tx writes. */
  tx?: Transaction;
}

export interface ScheduledTask {
  name: string;
  queue: string;
  cron: string;
  payload?: JsonValue;
  priority?: number;
  expiresS?: number;
}

export interface ScheduledFire {
  name: string;
  queue: string;
  fireAt: number;
  jobId: number;
}

export interface Notification {
  id: number;
  channel: string;
  payload: JsonValue;
}

export interface StreamEvent {
  offset: number;
  topic: string;
  key: string | null;
  payload: JsonValue;
  createdAt: number;
}

// ----- classes -----

export declare class Transaction {
  execute(sql: string, params?: JsonValue[]): number;
  query(sql: string, params?: JsonValue[]): Record<string, any>[];
  notify(channel: string, payload: JsonValue): number;
  commit(): void;
  rollback(): void;
}

export declare class Database {
  constructor(path: string, maxReaders?: number);

  // low-level
  transaction(): Transaction;
  query(sql: string, params?: JsonValue[]): Record<string, any>[];
  walEvents(): WalEvents;
  pruneNotifications(olderThanS?: number | null, maxKeep?: number | null): number;

  // queues / streams / scheduler
  queue(name: string, opts?: QueueOpts): Queue;
  stream(name: string): Stream;
  scheduler(): Scheduler;

  // pub/sub
  notify(channel: string, payload: JsonValue): number;
  notifyTx(tx: Transaction, channel: string, payload: JsonValue): number;
  listen(channel: string): ListenIterator;

  // locks / rate limits / results
  tryLock(name: string, owner: string, ttlS: number): Lock | null;
  tryRateLimit(name: string, limit: number, per: number): boolean;
  saveResult(jobId: number, value: string, ttlS: number): void;
  getResult(jobId: number): string | null;
  sweepResults(): number;
}

export declare class Queue {
  readonly name: string;
  readonly visibilityTimeoutS: number;
  readonly maxAttempts: number;

  enqueue(payload: JsonValue, opts?: EnqueueOpts): number;
  enqueueTx(tx: Transaction, payload: JsonValue, opts?: Omit<EnqueueOpts, 'tx'>): number;
  claimBatch(workerId: string, n: number): Job[];
  claimOne(workerId: string): Job | null;
  ackBatch(ids: number[], workerId: string): number;
  sweepExpired(): number;
  claimWaker(): ClaimWaker;
}

export declare class Job {
  readonly id: number;
  readonly queue: string;
  readonly payload: JsonValue;
  readonly workerId: string;
  readonly attempts: number;

  ack(): boolean;
  retry(delayS: number, error: string): boolean;
  fail(error: string): boolean;
  heartbeat(extendS: number): boolean;
}

export declare class ClaimWaker {
  next(workerId: string): Promise<Job | null>;
  close(): void;
}

export declare class Stream {
  readonly name: string;

  publish(payload: JsonValue): number;
  publishWithKey(key: string, payload: JsonValue): number;
  publishTx(tx: Transaction, payload: JsonValue): number;
  readSince(offset: number, limit: number): StreamEvent[];
  readFromConsumer(consumer: string, limit: number): StreamEvent[];
  saveOffset(consumer: string, offset: number): boolean;
  saveOffsetTx(tx: Transaction, consumer: string, offset: number): boolean;
  getOffset(consumer: string): number;
  subscribe(consumer: string): StreamSubscription;
}

export declare class StreamSubscription implements AsyncIterableIterator<StreamEvent> {
  readonly offset: number;
  next(): Promise<IteratorResult<StreamEvent>>;
  return?(value?: unknown): Promise<IteratorResult<StreamEvent>>;
  [Symbol.asyncIterator](): AsyncIterableIterator<StreamEvent>;
  saveOffset(): void;
  close(): void;
}

export declare class ListenIterator implements AsyncIterableIterator<Notification> {
  next(): Promise<IteratorResult<Notification>>;
  return?(value?: unknown): Promise<IteratorResult<Notification>>;
  [Symbol.asyncIterator](): AsyncIterableIterator<Notification>;
  close(): void;
}

export declare class Scheduler {
  add(task: ScheduledTask): void;
  remove(name: string): number;
  tick(): ScheduledFire[];
  soonest(): number;
  /**
   * Blocking leader loop. Elects via `honker-scheduler` advisory lock,
   * heartbeats every 20s, ticks every 1s. Pass an `AbortSignal` to
   * stop.
   */
  run(owner: string, signal?: AbortSignal): Promise<void>;
}

export declare class Lock {
  readonly name: string;
  readonly owner: string;
  readonly released: boolean;
  heartbeat(ttlS: number): boolean;
  release(): boolean;
}

export declare class WalEvents {
  next(): Promise<void>;
  close(): void;
}

export declare function open(path: string, maxReaders?: number): Database;
