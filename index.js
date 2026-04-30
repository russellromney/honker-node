'use strict';

const native = require('./native');

const value = (row) => (row && Object.prototype.hasOwnProperty.call(row, 'v') ? row.v : null);
const txNative = (tx) => (tx instanceof Transaction ? tx._native : tx);

function sleep(ms, signal) {
  return new Promise((resolve) => {
    if (signal?.aborted) return resolve();
    const timer = setTimeout(done, ms);
    function done() {
      signal?.removeEventListener('abort', done);
      resolve();
    }
    signal?.addEventListener('abort', () => {
      clearTimeout(timer);
      done();
    }, { once: true });
  });
}

function toStreamEvent(row) {
  return {
    offset: row.offset,
    topic: row.topic,
    key: row.key ?? null,
    payload: row.payload == null ? null : JSON.parse(row.payload),
    created_at: row.created_at,
  };
}

class Database {
  constructor(inner) {
    this._native = inner;
  }

  _requireOpen() {
    if (!this._native) throw new Error('Database is closed');
    return this._native;
  }

  close() {
    const nativeDb = this._native;
    this._native = null;
    nativeDb?.close();
  }
  transaction() { return new Transaction(this._requireOpen().transaction()); }
  query(sql, params) { return this._requireOpen().query(sql, params || null); }
  updateEvents() { return this._requireOpen().updateEvents(); }
  pruneNotifications(olderThanS, maxKeep) {
    return this._requireOpen().pruneNotifications(olderThanS ?? null, maxKeep ?? null);
  }

  _call(sql, params) {
    const tx = this.transaction();
    try {
      const rows = tx.query(sql, params);
      tx.commit();
      return rows;
    } catch (err) {
      tx.rollback();
      throw err;
    }
  }

  queue(name, opts = {}) {
    return new Queue(this, name, {
      visibilityTimeoutS: opts.visibilityTimeoutS ?? 300,
      maxAttempts: opts.maxAttempts ?? 3,
    });
  }
  stream(name) { return new Stream(this, name); }
  scheduler() { return new Scheduler(this); }

  notify(channel, payload) {
    const tx = this.transaction();
    try {
      const id = tx.notify(channel, payload);
      tx.commit();
      return id;
    } catch (err) {
      tx.rollback();
      throw err;
    }
  }
  notifyTx(tx, channel, payload) { return txNative(tx).notify(channel, payload); }
  listen(channel) { return new ListenIterator(this, channel); }
  tryLock(name, owner, ttlS) {
    return value(this._call('SELECT honker_lock_acquire(?, ?, ?) AS v', [name, owner, ttlS])[0]) === 1
      ? new Lock(this, name, owner)
      : null;
  }
  tryRateLimit(name, limit, per) {
    return value(this._call('SELECT honker_rate_limit_try(?, ?, ?) AS v', [name, limit, per])[0]) === 1;
  }
  saveResult(jobId, valueJson, ttlS = 0) {
    this._call('SELECT honker_result_save(?, ?, ?) AS v', [jobId, valueJson, ttlS]);
  }
  getResult(jobId) { return value(this._call('SELECT honker_result_get(?) AS v', [jobId])[0]); }
  sweepResults() { return value(this._call('SELECT honker_result_sweep() AS v')[0]); }
}

class Transaction {
  constructor(inner) {
    this._native = inner;
    this._done = false;
  }
  execute(sql, params) { return this._native.execute(sql, params || null); }
  query(sql, params) { return this._native.query(sql, params || null); }
  notify(channel, payload) { return this._native.notify(channel, payload); }
  commit() { if (!this._done) { this._native.commit(); this._done = true; } }
  rollback() { if (!this._done) { this._native.rollback(); this._done = true; } }
}

class Queue {
  constructor(db, name, opts) {
    this._db = db;
    this.name = name;
    this._opts = opts;
  }
  enqueue(payload, opts = {}) {
    const run = opts.tx ? txNative(opts.tx).query.bind(txNative(opts.tx)) : this._db._call.bind(this._db);
    return value(run('SELECT honker_enqueue(?, ?, ?, ?, ?, ?, ?) AS v', [
      this.name,
      JSON.stringify(payload),
      opts.runAt ?? null,
      opts.delay ?? null,
      opts.priority ?? 0,
      this._opts.maxAttempts,
      opts.expires ?? null,
    ])[0]);
  }
  enqueueTx(tx, payload, opts = {}) { return this.enqueue(payload, { ...opts, tx }); }
  claimBatch(workerId, n) {
    const row = this._db._call('SELECT honker_claim_batch(?, ?, ?, ?) AS v', [
      this.name,
      workerId,
      n,
      this._opts.visibilityTimeoutS,
    ])[0];
    return JSON.parse(value(row)).map((r) => new Job(this._db, r));
  }
  claimOne(workerId) { return this.claimBatch(workerId, 1)[0] ?? null; }
  ackBatch(ids, workerId) {
    return value(this._db._call('SELECT honker_ack_batch(?, ?) AS v', [JSON.stringify(ids), workerId])[0]);
  }
  sweepExpired() { return value(this._db._call('SELECT honker_sweep_expired(?) AS v', [this.name])[0]); }
  claimWaker() { return new ClaimWaker(this); }
}

class Job {
  constructor(db, row) {
    this._db = db;
    this.id = row.id;
    this.queue = row.queue;
    this.payload = JSON.parse(row.payload);
    this.workerId = row.worker_id;
    this.attempts = row.attempts;
  }
  ack() { return value(this._db._call('SELECT honker_ack(?, ?) AS v', [this.id, this.workerId])[0]) > 0; }
  retry(delaySec, errorMsg) {
    return value(this._db._call('SELECT honker_retry(?, ?, ?, ?) AS v', [this.id, this.workerId, delaySec, errorMsg])[0]) > 0;
  }
  fail(errorMsg) {
    return value(this._db._call('SELECT honker_fail(?, ?, ?) AS v', [this.id, this.workerId, errorMsg])[0]) > 0;
  }
  heartbeat(extendSec) {
    return value(this._db._call('SELECT honker_heartbeat(?, ?, ?) AS v', [this.id, this.workerId, extendSec])[0]) > 0;
  }
}

class ClaimWaker {
  constructor(queue) {
    this._queue = queue;
    this._ev = queue._db.updateEvents();
    this._closed = false;
  }
  tryNext(workerId) { return this._queue.claimOne(workerId); }
  async next(workerId, opts = {}) {
    while (!this._closed && !opts.signal?.aborted) {
      const job = this.tryNext(workerId);
      if (job) return job;
      try { await Promise.race([this._ev.next(), sleep(5000, opts.signal)]); } catch { return null; }
    }
    return null;
  }
  close() { if (!this._closed) { this._closed = true; this._ev.close(); } }
}

class Stream {
  constructor(db, topic) {
    this._db = db;
    this.topic = topic;
  }
  publish(payload) { return this._publish(null, payload, this._db._call.bind(this._db)); }
  publishWithKey(key, payload) { return this._publish(key, payload, this._db._call.bind(this._db)); }
  publishTx(tx, payload, key = null) { return this._publish(key, payload, txNative(tx).query.bind(txNative(tx))); }
  _publish(key, payload, run) {
    return value(run('SELECT honker_stream_publish(?, ?, ?) AS v', [this.topic, key, JSON.stringify(payload)])[0]);
  }
  readSince(offset, limit) {
    return JSON.parse(value(this._db._call('SELECT honker_stream_read_since(?, ?, ?) AS v', [this.topic, offset, limit])[0])).map(toStreamEvent);
  }
  readFromConsumer(consumer, limit) { return this.readSince(this.getOffset(consumer), limit); }
  saveOffset(consumer, offset) { return this._saveOffset(consumer, offset, this._db._call.bind(this._db)); }
  saveOffsetTx(tx, consumer, offset) { return this._saveOffset(consumer, offset, txNative(tx).query.bind(txNative(tx))); }
  _saveOffset(consumer, offset, run) {
    return value(run('SELECT honker_stream_save_offset(?, ?, ?) AS v', [consumer, this.topic, offset])[0]) > 0;
  }
  getOffset(consumer) {
    return value(this._db._call('SELECT honker_stream_get_offset(?, ?) AS v', [consumer, this.topic])[0]) ?? 0;
  }
  subscribe(consumer, opts = {}) { return new StreamSubscription(this, consumer, opts.saveEveryN ?? 1000); }
}

class StreamSubscription {
  constructor(stream, consumer, saveEveryN) {
    this._stream = stream;
    this._consumer = consumer;
    this._saveEveryN = saveEveryN;
    this._offset = stream.getOffset(consumer);
    this._lastSaved = this._offset;
    this._pending = [];
    this._ev = stream._db.updateEvents();
    this._closed = false;
  }
  [Symbol.asyncIterator]() { return this; }
  async next() {
    while (!this._closed) {
      if (this._pending.length) {
        const ev = this._pending.shift();
        this._offset = ev.offset;
        if (this._saveEveryN > 0 && this._offset - this._lastSaved >= this._saveEveryN) this._flush();
        return { done: false, value: ev };
      }
      this._pending = this._stream.readSince(this._offset, 100);
      if (this._pending.length) continue;
      try { await this._ev.next(); } catch { return { done: true }; }
    }
    return { done: true };
  }
  _flush() {
    if (this._offset > this._lastSaved) {
      this._stream.saveOffset(this._consumer, this._offset);
      this._lastSaved = this._offset;
    }
  }
  close() { if (!this._closed) { this._closed = true; this._flush(); this._ev.close(); } }
  async return() { this.close(); return { done: true }; }
}

class ListenIterator {
  constructor(db, channel) {
    this._db = db;
    this._channel = channel;
    this._lastId = value(db.query('SELECT COALESCE(MAX(id), 0) AS v FROM _honker_notifications')[0]);
    this._pending = [];
    this._ev = db.updateEvents();
    this._closed = false;
  }
  [Symbol.asyncIterator]() { return this; }
  async next() {
    while (!this._closed) {
      if (this._pending.length) return { done: false, value: this._pending.shift() };
      const rows = this._db.query(
        'SELECT id, channel, payload FROM _honker_notifications WHERE id > ? AND channel = ? ORDER BY id ASC LIMIT 1000',
        [this._lastId, this._channel],
      );
      for (const r of rows) {
        this._lastId = r.id;
        this._pending.push({ id: r.id, channel: r.channel, payload: r.payload == null ? null : JSON.parse(r.payload) });
      }
      if (this._pending.length) continue;
      try { await this._ev.next(); } catch { return { done: true }; }
    }
    return { done: true };
  }
  close() { if (!this._closed) { this._closed = true; this._ev.close(); } }
  async return() { this.close(); return { done: true }; }
}

class Scheduler {
  constructor(db) { this._db = db; }
  add(task) {
    this._db._call('SELECT honker_scheduler_register(?, ?, ?, ?, ?, ?) AS v', [
      task.name,
      task.queue,
      task.cron,
      JSON.stringify(task.payload),
      task.priority ?? 0,
      task.expiresS ?? null,
    ]);
  }
  remove(name) { return value(this._db._call('SELECT honker_scheduler_unregister(?) AS v', [name])[0]); }
  tick(now = Math.floor(Date.now() / 1000)) {
    return JSON.parse(value(this._db._call('SELECT honker_scheduler_tick(?) AS v', [now])[0]));
  }
  soonest() { return value(this._db._call('SELECT honker_scheduler_soonest() AS v')[0]) ?? 0; }
  async run(owner, signal) {
    while (!signal?.aborted) {
      const lock = this._db.tryLock('honker-scheduler', owner, 60);
      if (!lock) { await sleep(5000, signal); continue; }
      try {
        while (!signal?.aborted) { this.tick(); await sleep(1000, signal); }
      } finally {
        lock.release();
      }
    }
  }
}

class Lock {
  constructor(db, name, owner) {
    this._db = db;
    this.name = name;
    this.owner = owner;
    this.released = false;
  }
  release() {
    if (this.released) return false;
    this.released = true;
    return value(this._db._call('SELECT honker_lock_release(?, ?) AS v', [this.name, this.owner])[0]) > 0;
  }
  heartbeat(ttlS) {
    const tx = this._db.transaction();
    try {
      tx.execute('UPDATE _honker_locks SET expires_at = unixepoch() + ? WHERE name = ? AND owner = ?', [ttlS, this.name, this.owner]);
      const ok = tx.query('SELECT changes() AS c')[0].c > 0;
      tx.commit();
      return ok;
    } catch (err) {
      tx.rollback();
      throw err;
    }
  }
}

function open(path, maxReaders) {
  return new Database(native.open(path, maxReaders ?? null));
}

if (Symbol.dispose) {
  Database.prototype[Symbol.dispose] = Database.prototype.close;
  ClaimWaker.prototype[Symbol.dispose] = ClaimWaker.prototype.close;
  StreamSubscription.prototype[Symbol.dispose] = StreamSubscription.prototype.close;
  ListenIterator.prototype[Symbol.dispose] = ListenIterator.prototype.close;
}

if (Symbol.asyncDispose) {
  Database.prototype[Symbol.asyncDispose] = async function asyncDisposeDb() {
    this.close();
  };
  ClaimWaker.prototype[Symbol.asyncDispose] = async function asyncDisposeWaker() {
    this.close();
  };
  StreamSubscription.prototype[Symbol.asyncDispose] =
    async function asyncDisposeSubscription() {
      this.close();
    };
  ListenIterator.prototype[Symbol.asyncDispose] = async function asyncDisposeListener() {
    this.close();
  };
}

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
  UpdateEvents: native.UpdateEvents,
  native,
};
