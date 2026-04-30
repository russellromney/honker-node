'use strict';

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

function sleepSync(ms) {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

function cleanupDir(dir) {
  global.gc?.();
  sleepSync(100);
  let lastErr;
  for (let i = 0; i < 60; i++) {
    try {
      fs.rmSync(dir, { recursive: true, force: true });
      return;
    } catch (err) {
      if (!['EBUSY', 'EPERM', 'ENOTEMPTY'].includes(err.code)) throw err;
      lastErr = err;
      global.gc?.();
      sleepSync(50 * (i + 1));
    }
  }
  throw lastErr;
}

function trackCloseable(tracked, resource) {
  if (resource && typeof resource.close === 'function') tracked.push(resource);
  return resource;
}

function wrapDatabase(db, tracked) {
  if (db.__honkerTracked) return db;

  Object.defineProperty(db, '__honkerTracked', {
    value: true,
    enumerable: false,
    configurable: false,
    writable: false,
  });

  trackCloseable(tracked, db);

  const realUpdateEvents = db.updateEvents.bind(db);
  db.updateEvents = (...args) => trackCloseable(tracked, realUpdateEvents(...args));

  const realListen = db.listen.bind(db);
  db.listen = (...args) => trackCloseable(tracked, realListen(...args));

  const realStream = db.stream.bind(db);
  db.stream = (...args) => {
    const stream = realStream(...args);
    const realSubscribe = stream.subscribe.bind(stream);
    stream.subscribe = (...subArgs) => trackCloseable(tracked, realSubscribe(...subArgs));
    return stream;
  };

  const realQueue = db.queue.bind(db);
  db.queue = (...args) => {
    const queue = realQueue(...args);
    if (typeof queue.claimWaker === 'function') {
      const realClaimWaker = queue.claimWaker.bind(queue);
      queue.claimWaker = (...wakeArgs) =>
        trackCloseable(tracked, realClaimWaker(...wakeArgs));
    }
    return queue;
  };

  return db;
}

function closeTracked(tracked) {
  const seen = new Set();
  while (tracked.length) {
    const resource = tracked.pop();
    if (!resource || seen.has(resource)) continue;
    seen.add(resource);
    try {
      resource.close?.();
    } catch {}
  }
  global.gc?.();
  sleepSync(100);
}

function createTempDb(prefix, openFn) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  const tracked = [];
  return {
    path: path.join(dir, 't.db'),
    dir,
    open: (...args) => wrapDatabase(openFn(...args), tracked),
    cleanup: () => {
      closeTracked(tracked);
      cleanupDir(dir);
    },
  };
}

module.exports = {
  cleanupDir,
  createTempDb,
  sleepSync,
};
