'use strict';

// High-level wrapper over the napi-rs native binding. The native layer
// (see ./native.js -> src/lib.rs) exposes only the low-level primitives:
// open(), Database.{transaction,query,walEvents,pruneNotifications},
// Transaction.{execute,query,notify,commit,rollback}. Everything a user
// wants — queues, streams, scheduler, locks, rate limits, results, listen —
// is a small JS wrapper on top that calls honker_* SQL functions.
//
// Mirrors the shape of the Rust (honker), Python, Ruby, and Elixir
// bindings. Patterns (recv-then-drain, leader election, stream
// auto-save) are copied from honker-rs where the canonical reference
// lives; see ../honker-rs/src/lib.rs for line-level justifications.

const native = require('./native.js');

// ---------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------

function singleTxQuery(db, sql, params) {
  // Helper for one-shot query-returning SQL that needs to run inside
  // a transaction (every honker_* function writes _honker_* rows).
  const tx = db._native.transaction();
  try {
    const rows = tx.query(sql, params);
    tx.commit();
    return rows;
  } catch (e) {
    try {
      tx.rollback();
    } catch {
      /* already rolled back */
    }
    throw e;
  }
}

function scalar(rows) {
  if (rows.length === 0) return null;
  const row = rows[0];
  const keys = Object.keys(row);
  return keys.length === 0 ? null : row[keys[0]];
}

function nowUnix() {
  return Math.floor(Date.now() / 1000);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------
// Transaction wrapper — thin passthrough plus typed helpers
// ---------------------------------------------------------------------

class Transaction {
  constructor(nativeTx) {
    this._tx = nativeTx;
    this._finished = false;
  }

  execute(sql, params) {
    return this._tx.execute(sql, params || null);
  }

  query(sql, params) {
    return this._tx.query(sql, params || null);
  }

  notify(channel, payload) {
    return this._tx.notify(channel, payload);
  }

  commit() {
    if (this._finished) return;
    this._finished = true;
    this._tx.commit();
  }

  rollback() {
    if (this._finished) return;
    this._finished = true;
    this._tx.rollback();
  }
}

// ---------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------

class Database {
  constructor(path, maxReaders) {
    this._native = native.open(path, maxReaders ?? null);
    // Auto-bootstrap the schema so callers don't have to remember to
    // call honker_bootstrap() themselves. Matches honker-rs, honker-py,
    // honker-ruby, honker-ex behavior.
    const tx = this._native.transaction();
    try {
      tx.query('SELECT honker_bootstrap()', null);
      tx.commit();
    } catch (e) {
      try {
        tx.rollback();
      } catch {
        /* ignore */
      }
      throw e;
    }
  }

  // ---- low-level passthroughs ----

  transaction() {
    return new Transaction(this._native.transaction());
  }

  query(sql, params) {
    return this._native.query(sql, params || null);
  }

  walEvents() {
    return this._native.walEvents();
  }

  pruneNotifications(olderThanS, maxKeep) {
    return this._native.pruneNotifications(olderThanS ?? null, maxKeep ?? null);
  }

  // ---- queues ----

  queue(name, opts) {
    return new Queue(this, name, opts);
  }

  // ---- streams ----

  stream(name) {
    return new Stream(this, name);
  }

  // ---- scheduler ----

  scheduler() {
    return new Scheduler(this);
  }

  // ---- pub/sub ----

  notify(channel, payload) {
    // Fire a notification on its own transaction. Returns the row id.
    const tx = this._native.transaction();
    try {
      const id = tx.notify(channel, payload);
      tx.commit();
      return id;
    } catch (e) {
      try {
        tx.rollback();
      } catch {
        /* ignore */
      }
      throw e;
    }
  }

  notifyTx(tx, channel, payload) {
    // Convenience: same as tx.notify() but reads symmetrically with
    // enqueueTx / publishTx.
    return tx.notify(channel, payload);
  }

  listen(channel) {
    return new ListenIterator(this, channel);
  }

  // ---- advisory locks ----

  tryLock(name, owner, ttlS) {
    const rows = singleTxQuery(
      this,
      'SELECT honker_lock_acquire(?, ?, ?) AS acquired',
      [name, owner, ttlS],
    );
    const got = scalar(rows);
    if (got === 1) {
      return new Lock(this, name, owner);
    }
    return null;
  }

  // ---- rate limits ----

  tryRateLimit(name, limit, per) {
    const rows = singleTxQuery(
      this,
      'SELECT honker_rate_limit_try(?, ?, ?) AS ok',
      [name, limit, per],
    );
    return scalar(rows) === 1;
  }

  // ---- results ----

  saveResult(jobId, value, ttlS) {
    singleTxQuery(
      this,
      'SELECT honker_result_save(?, ?, ?)',
      [jobId, value, ttlS],
    );
  }

  getResult(jobId) {
    const rows = singleTxQuery(
      this,
      'SELECT honker_result_get(?) AS v',
      [jobId],
    );
    const v = scalar(rows);
    return v === null || v === undefined ? null : v;
  }

  sweepResults() {
    const rows = singleTxQuery(
      this,
      'SELECT honker_result_sweep() AS n',
      [],
    );
    return scalar(rows) || 0;
  }
}

function open(path, maxReaders) {
  return new Database(path, maxReaders);
}

// ---------------------------------------------------------------------
// Queue / Job / ClaimWaker
// ---------------------------------------------------------------------

class Queue {
  constructor(db, name, opts) {
    this._db = db;
    this.name = name;
    const o = opts || {};
    this.visibilityTimeoutS = o.visibilityTimeoutS ?? 300;
    this.maxAttempts = o.maxAttempts ?? 3;
  }

  _enqueueOn(txOrNull, payload, opts) {
    const o = opts || {};
    const payloadJson = JSON.stringify(payload);
    const params = [
      this.name,
      payloadJson,
      o.runAt ?? null,
      o.delay ?? null,
      o.priority ?? 0,
      this.maxAttempts,
      o.expires ?? null,
    ];
    const sql = 'SELECT honker_enqueue(?, ?, ?, ?, ?, ?, ?) AS id';
    if (txOrNull) {
      const rows = txOrNull.query(sql, params);
      return scalar(rows);
    }
    const rows = singleTxQuery(this._db, sql, params);
    return scalar(rows);
  }

  enqueue(payload, opts) {
    const o = opts || {};
    if (o.tx) {
      const { tx, ...rest } = o;
      return this._enqueueOn(tx, payload, rest);
    }
    return this._enqueueOn(null, payload, o);
  }

  enqueueTx(tx, payload, opts) {
    return this._enqueueOn(tx, payload, opts || {});
  }

  claimBatch(workerId, n) {
    const rows = singleTxQuery(
      this._db,
      'SELECT honker_claim_batch(?, ?, ?, ?) AS j',
      [this.name, workerId, n, this.visibilityTimeoutS],
    );
    const json = scalar(rows);
    if (!json) return [];
    const raw = JSON.parse(json);
    return raw.map((r) => new Job(this._db, r));
  }

  claimOne(workerId) {
    const jobs = this.claimBatch(workerId, 1);
    return jobs.length ? jobs[0] : null;
  }

  ackBatch(ids, workerId) {
    const rows = singleTxQuery(
      this._db,
      'SELECT honker_ack_batch(?, ?) AS n',
      [JSON.stringify(ids), workerId],
    );
    return scalar(rows) || 0;
  }

  sweepExpired() {
    const rows = singleTxQuery(
      this._db,
      'SELECT honker_sweep_expired(?) AS n',
      [this.name],
    );
    return scalar(rows) || 0;
  }

  claimWaker() {
    return new ClaimWaker(this);
  }
}

class Job {
  constructor(db, raw) {
    this._db = db;
    this.id = raw.id;
    this.queue = raw.queue;
    // Payload arrives as a JSON *string* inside the JSON array returned
    // by honker_claim_batch. Decode once so callers get plain values.
    this.payload = typeof raw.payload === 'string'
      ? safeParseJson(raw.payload)
      : raw.payload;
    this.workerId = raw.worker_id;
    this.attempts = raw.attempts;
  }

  ack() {
    const rows = singleTxQuery(
      this._db,
      'SELECT honker_ack(?, ?) AS n',
      [this.id, this.workerId],
    );
    return (scalar(rows) || 0) > 0;
  }

  retry(delayS, error) {
    const rows = singleTxQuery(
      this._db,
      'SELECT honker_retry(?, ?, ?, ?) AS n',
      [this.id, this.workerId, delayS, error],
    );
    return (scalar(rows) || 0) > 0;
  }

  fail(error) {
    const rows = singleTxQuery(
      this._db,
      'SELECT honker_fail(?, ?, ?) AS n',
      [this.id, this.workerId, error],
    );
    return (scalar(rows) || 0) > 0;
  }

  heartbeat(extendS) {
    const rows = singleTxQuery(
      this._db,
      'SELECT honker_heartbeat(?, ?, ?) AS n',
      [this.id, this.workerId, extendS],
    );
    return (scalar(rows) || 0) > 0;
  }
}

function safeParseJson(s) {
  try {
    return JSON.parse(s);
  } catch {
    return s;
  }
}

// WAL-wake claim helper. Recv-then-drain: await the next tick FIRST,
// then drain any extra ticks that piled up. Drain-before-recv would
// lose a tick that landed between try-claim and drain.
class ClaimWaker {
  constructor(queue) {
    this._queue = queue;
    this._ev = queue._db._native.walEvents();
    this._closed = false;
  }

  async next(workerId) {
    while (!this._closed) {
      const jobs = this._queue.claimBatch(workerId, 1);
      if (jobs.length) return jobs[0];
      try {
        await this._ev.next();
      } catch {
        return null;
      }
      // No explicit drain primitive in the napi WalEvents surface —
      // the shared-watcher's internal channel coalesces under load
      // naturally; the next loop iteration re-tries the claim.
    }
    return null;
  }

  close() {
    if (this._closed) return;
    this._closed = true;
    try {
      this._ev.close();
    } catch {
      /* ignore */
    }
  }
}

// ---------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------

class Stream {
  constructor(db, name) {
    this._db = db;
    this.name = name;
  }

  publish(payload) {
    return this._publish(null, null, payload);
  }

  publishWithKey(key, payload) {
    return this._publish(null, key, payload);
  }

  publishTx(tx, payload) {
    return this._publish(tx, null, payload);
  }

  _publish(txOrNull, key, payload) {
    const payloadJson = JSON.stringify(payload);
    const sql = 'SELECT honker_stream_publish(?, ?, ?) AS o';
    const params = [this.name, key, payloadJson];
    if (txOrNull) {
      const rows = txOrNull.query(sql, params);
      return scalar(rows);
    }
    const rows = singleTxQuery(this._db, sql, params);
    return scalar(rows);
  }

  readSince(offset, limit) {
    const rows = singleTxQuery(
      this._db,
      'SELECT honker_stream_read_since(?, ?, ?) AS j',
      [this.name, offset, limit],
    );
    const json = scalar(rows);
    if (!json) return [];
    const raw = JSON.parse(json);
    return raw.map((r) => ({
      offset: r.offset,
      topic: r.topic,
      key: r.key ?? null,
      payload: safeParseJson(r.payload),
      createdAt: r.created_at,
    }));
  }

  readFromConsumer(consumer, limit) {
    const off = this.getOffset(consumer);
    return this.readSince(off, limit);
  }

  saveOffset(consumer, offset) {
    const rows = singleTxQuery(
      this._db,
      'SELECT honker_stream_save_offset(?, ?, ?) AS n',
      [consumer, this.name, offset],
    );
    return (scalar(rows) || 0) > 0;
  }

  saveOffsetTx(tx, consumer, offset) {
    const rows = tx.query(
      'SELECT honker_stream_save_offset(?, ?, ?) AS n',
      [consumer, this.name, offset],
    );
    return (scalar(rows) || 0) > 0;
  }

  getOffset(consumer) {
    const rows = singleTxQuery(
      this._db,
      'SELECT honker_stream_get_offset(?, ?) AS o',
      [consumer, this.name],
    );
    return scalar(rows) || 0;
  }

  subscribe(consumer) {
    return new StreamSubscription(this, consumer);
  }
}

// Async iterable of stream events for a named consumer. Saves offset
// every 1000 events and again on close. Recv-then-drain pattern to
// avoid missing a publish between refill() and recv.
class StreamSubscription {
  constructor(stream, consumer) {
    this._stream = stream;
    this._consumer = consumer;
    this._ev = stream._db._native.walEvents();
    this._lastOffset = stream.getOffset(consumer);
    this._lastSaved = this._lastOffset;
    this._saveEveryN = 1000;
    this._pending = [];
    this._closed = false;
  }

  [Symbol.asyncIterator]() {
    return this;
  }

  async next() {
    while (!this._closed) {
      if (this._pending.length) {
        const ev = this._pending.shift();
        this._lastOffset = ev.offset;
        if (
          this._saveEveryN > 0 &&
          this._lastOffset - this._lastSaved >= this._saveEveryN
        ) {
          this._autoSave();
        }
        return { value: ev, done: false };
      }
      this._refill();
      if (this._pending.length) continue;
      try {
        await this._ev.next();
      } catch {
        this._closed = true;
        break;
      }
    }
    this._autoSave();
    return { value: undefined, done: true };
  }

  _refill() {
    const rows = singleTxQuery(
      this._stream._db,
      'SELECT honker_stream_read_since(?, ?, ?) AS j',
      [this._stream.name, this._lastOffset, 100],
    );
    const json = scalar(rows);
    if (!json) return;
    const raw = JSON.parse(json);
    for (const r of raw) {
      this._pending.push({
        offset: r.offset,
        topic: r.topic,
        key: r.key ?? null,
        payload: safeParseJson(r.payload),
        createdAt: r.created_at,
      });
    }
  }

  _autoSave() {
    if (this._lastOffset > this._lastSaved) {
      try {
        const ok = this._stream.saveOffset(this._consumer, this._lastOffset);
        if (ok) this._lastSaved = this._lastOffset;
      } catch {
        /* best-effort */
      }
    }
  }

  saveOffset() {
    this._autoSave();
  }

  get offset() {
    return this._lastOffset;
  }

  close() {
    if (this._closed) return;
    this._closed = true;
    this._autoSave();
    try {
      this._ev.close();
    } catch {
      /* ignore */
    }
  }

  return() {
    this.close();
    return Promise.resolve({ value: undefined, done: true });
  }
}

// ---------------------------------------------------------------------
// Listen (pub/sub subscription)
// ---------------------------------------------------------------------

class ListenIterator {
  constructor(db, channel) {
    this._db = db;
    this._channel = channel;
    this._ev = db._native.walEvents();
    // Start from the current max id so we don't replay history.
    const rows = db._native.query(
      'SELECT COALESCE(MAX(id), 0) AS m FROM _honker_notifications',
      null,
    );
    this._lastId = rows.length ? rows[0].m : 0;
    this._pending = [];
    this._closed = false;
  }

  [Symbol.asyncIterator]() {
    return this;
  }

  async next() {
    while (!this._closed) {
      if (this._pending.length) {
        const n = this._pending.shift();
        this._lastId = n.id;
        return { value: n, done: false };
      }
      this._refill();
      if (this._pending.length) continue;
      try {
        await this._ev.next();
      } catch {
        this._closed = true;
        break;
      }
    }
    return { value: undefined, done: true };
  }

  _refill() {
    const rows = this._db._native.query(
      'SELECT id, channel, payload FROM _honker_notifications ' +
        'WHERE id > ? AND channel = ? ORDER BY id ASC LIMIT 1000',
      [this._lastId, this._channel],
    );
    for (const r of rows) {
      this._pending.push({
        id: r.id,
        channel: r.channel,
        payload: safeParseJson(r.payload),
      });
    }
  }

  close() {
    if (this._closed) return;
    this._closed = true;
    try {
      this._ev.close();
    } catch {
      /* ignore */
    }
  }

  return() {
    this.close();
    return Promise.resolve({ value: undefined, done: true });
  }
}

// ---------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------

const SCHEDULER_LOCK_NAME = 'honker-scheduler';
const SCHEDULER_LOCK_TTL_S = 60;
const SCHEDULER_HEARTBEAT_MS = 20_000;
const SCHEDULER_TICK_MS = 1_000;
const SCHEDULER_STANDBY_MS = 5_000;

class Scheduler {
  constructor(db) {
    this._db = db;
  }

  add(task) {
    if (!task || !task.name || !task.queue || !task.cron) {
      throw new TypeError(
        'scheduler.add requires {name, queue, cron, payload}',
      );
    }
    singleTxQuery(
      this._db,
      'SELECT honker_scheduler_register(?, ?, ?, ?, ?, ?)',
      [
        task.name,
        task.queue,
        task.cron,
        JSON.stringify(task.payload ?? null),
        task.priority ?? 0,
        task.expiresS ?? null,
      ],
    );
  }

  remove(name) {
    const rows = singleTxQuery(
      this._db,
      'SELECT honker_scheduler_unregister(?) AS n',
      [name],
    );
    return scalar(rows) || 0;
  }

  tick() {
    const rows = singleTxQuery(
      this._db,
      'SELECT honker_scheduler_tick(?) AS j',
      [nowUnix()],
    );
    const json = scalar(rows);
    if (!json) return [];
    const raw = JSON.parse(json);
    return raw.map((r) => ({
      name: r.name,
      queue: r.queue,
      fireAt: r.fire_at,
      jobId: r.job_id,
    }));
  }

  soonest() {
    const rows = singleTxQuery(
      this._db,
      'SELECT honker_scheduler_soonest() AS t',
      [],
    );
    return scalar(rows) || 0;
  }

  // Blocking leader loop. `signal` is an AbortSignal; the loop exits
  // when it fires. Leader election via honker-scheduler advisory lock;
  // heartbeat every 20s. If heartbeat returns 0 (lock stolen after
  // TTL), exit the leader loop and re-contest. On tick error we
  // release the lock BEFORE throwing so a standby can pick up
  // without waiting for the TTL.
  async run(owner, signal) {
    const stopped = () => Boolean(signal && signal.aborted);
    while (!stopped()) {
      const lock = this._db.tryLock(
        SCHEDULER_LOCK_NAME,
        owner,
        SCHEDULER_LOCK_TTL_S,
      );
      if (!lock) {
        await sleepAbortable(SCHEDULER_STANDBY_MS, signal);
        continue;
      }
      let lostLock = false;
      let tickError = null;
      const t0 = Date.now();
      let lastHeartbeat = t0;
      try {
        while (!stopped()) {
          try {
            this.tick();
          } catch (e) {
            tickError = e;
            break;
          }
          if (Date.now() - lastHeartbeat >= SCHEDULER_HEARTBEAT_MS) {
            const stillOurs = lock.heartbeat(SCHEDULER_LOCK_TTL_S);
            if (!stillOurs) {
              lostLock = true;
              break;
            }
            lastHeartbeat = Date.now();
          }
          await sleepAbortable(SCHEDULER_TICK_MS, signal);
        }
      } finally {
        if (!lostLock) {
          try {
            lock.release();
          } catch {
            /* ignore */
          }
        }
      }
      if (tickError) throw tickError;
    }
  }
}

// Abort-aware sleep. Wakes early when the signal fires. Sleeps in
// short slices so the abort is observed within ~100ms regardless of
// how large `ms` is. Compatible with undefined `signal`.
async function sleepAbortable(ms, signal) {
  if (signal && signal.aborted) return;
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    if (signal && signal.aborted) return;
    const slice = Math.min(100, deadline - Date.now());
    if (slice <= 0) return;
    await sleep(slice);
  }
}

// ---------------------------------------------------------------------
// Lock
// ---------------------------------------------------------------------

// Best-effort cleanup via FinalizationRegistry. Explicit release() is
// strongly preferred — finalizers are not guaranteed to run.
const lockFinalizers = new FinalizationRegistry(({ db, name, owner }) => {
  try {
    singleTxQuery(db, 'SELECT honker_lock_release(?, ?)', [name, owner]);
  } catch {
    /* ignore */
  }
});

class Lock {
  constructor(db, name, owner) {
    this._db = db;
    this.name = name;
    this.owner = owner;
    this._released = false;
    this._token = { db, name, owner };
    lockFinalizers.register(this, this._token, this);
  }

  heartbeat(ttlS) {
    const rows = singleTxQuery(
      this._db,
      'SELECT honker_lock_acquire(?, ?, ?) AS n',
      [this.name, this.owner, ttlS],
    );
    return scalar(rows) === 1;
  }

  release() {
    if (this._released) return false;
    this._released = true;
    lockFinalizers.unregister(this);
    const rows = singleTxQuery(
      this._db,
      'SELECT honker_lock_release(?, ?) AS n',
      [this.name, this.owner],
    );
    return scalar(rows) === 1;
  }

  get released() {
    return this._released;
  }
}

// ---------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------

module.exports = {
  open,
  Database,
  Transaction,
  Queue,
  Job,
  ClaimWaker,
  Stream,
  StreamSubscription,
  Scheduler,
  Lock,
  ListenIterator,
  // Expose the native binding so advanced users can reach WalEvents
  // directly if they need raw wake ticks without our wrappers.
  native,
};
