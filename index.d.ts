export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

export interface QueueOpts { visibilityTimeoutS?: number; maxAttempts?: number; }
export interface EnqueueOpts { delay?: number | null; runAt?: number | null; priority?: number; expires?: number | null; tx?: Transaction; }
export interface ScheduledTask { name: string; queue: string; cron: string; payload: JsonValue; priority?: number; expiresS?: number | null; }
export interface ScheduledFire { name: string; queue: string; fire_at: number; job_id: number; }
export interface StreamEvent { offset: number; topic: string; key: string | null; payload: JsonValue; created_at: number; }
export interface Notification { id: number; channel: string; payload: JsonValue; }

export declare class Database {
  transaction(): Transaction;
  query(sql: string, params?: JsonValue[]): Record<string, any>[];
  updateEvents(): UpdateEvents;
  pruneNotifications(olderThanS?: number | null, maxKeep?: number | null): number;
  queue(name: string, opts?: QueueOpts): Queue;
  stream(name: string): Stream;
  scheduler(): Scheduler;
  notify(channel: string, payload: JsonValue): number;
  notifyTx(tx: Transaction, channel: string, payload: JsonValue): number;
  listen(channel: string): AsyncIterableIterator<Notification> & { close(): void };
  tryLock(name: string, owner: string, ttlS: number): Lock | null;
  tryRateLimit(name: string, limit: number, per: number): boolean;
  saveResult(jobId: number, valueJson: string, ttlS?: number): void;
  getResult(jobId: number): string | null;
  sweepResults(): number;
}

export declare class Transaction {
  execute(sql: string, params?: JsonValue[]): number;
  query(sql: string, params?: JsonValue[]): Record<string, any>[];
  notify(channel: string, payload: JsonValue): number;
  commit(): void;
  rollback(): void;
}

export declare class Queue {
  readonly name: string;
  enqueue(payload: JsonValue, opts?: EnqueueOpts): number;
  enqueueTx(tx: Transaction, payload: JsonValue, opts?: EnqueueOpts): number;
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
  retry(delaySec: number, errorMsg: string): boolean;
  fail(errorMsg: string): boolean;
  heartbeat(extendSec: number): boolean;
}

export declare class ClaimWaker {
  tryNext(workerId: string): Job | null;
  next(workerId: string, opts?: { signal?: AbortSignal }): Promise<Job | null>;
  close(): void;
}

export declare class Stream {
  readonly topic: string;
  publish(payload: JsonValue): number;
  publishWithKey(key: string, payload: JsonValue): number;
  publishTx(tx: Transaction, payload: JsonValue, key?: string | null): number;
  readSince(offset: number, limit: number): StreamEvent[];
  readFromConsumer(consumer: string, limit: number): StreamEvent[];
  saveOffset(consumer: string, offset: number): boolean;
  saveOffsetTx(tx: Transaction, consumer: string, offset: number): boolean;
  getOffset(consumer: string): number;
  subscribe(consumer: string, opts?: { saveEveryN?: number }): AsyncIterableIterator<StreamEvent> & { close(): void };
}

export declare class Scheduler {
  add(task: ScheduledTask): void;
  remove(name: string): number;
  tick(now?: number): ScheduledFire[];
  soonest(): number;
  run(owner: string, signal?: AbortSignal): Promise<void>;
}

export declare class Lock {
  readonly name: string;
  readonly owner: string;
  readonly released: boolean;
  heartbeat(ttlS: number): boolean;
  release(): boolean;
}

export declare class UpdateEvents { next(): Promise<void>; close(): void; }
export declare function open(path: string, maxReaders?: number): Database;
export declare const native: unknown;
