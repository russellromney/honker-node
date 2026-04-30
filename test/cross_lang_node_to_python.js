'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');

const honker = require('..');
const { createTempDb } = require('./helpers');
const {
  PACKAGES,
  createLineReader,
  spawnPython,
  stopChild,
} = require('./cross_lang_shared');

test(
  'direct proof: node writes notifications; python listen() observes them',
  async () => {
    const { path: dbPath, open, cleanup } = createTempDb(
      'xlang-node-to-py-',
      honker.open.bind(honker),
    );
    let db;
    let proc;
    try {
      db = open(dbPath);

      const pyScript = `
import asyncio, json, sys, time
sys.path.insert(0, ${JSON.stringify(PACKAGES)})
import honker

def diag(msg):
    sys.stderr.write("[py-diag] " + msg + "\\n")
    sys.stderr.flush()

async def main():
    db = honker.open(${JSON.stringify(dbPath)})
    diag("opened db, journal_mode=" + repr(db.query("PRAGMA journal_mode")))
    lst = db.listen("reverse")
    diag("listener created, last_seen=" + repr(lst._last_seen))
    print("READY", flush=True)
    got = []

    async def consume():
        async for n in lst:
            diag("consume got id=" + str(n.id))
            got.append(n.payload)
            if len(got) == 3:
                return

    async def diagnostics():
        # Independent reader-pool view. Tells us whether Python's
        # READER sees Node's writes, separate from whether the
        # WATCHER fires the wake.
        t0 = time.monotonic()
        i = 0
        while True:
            await asyncio.sleep(0.5)
            i += 1
            try:
                rows = db.query(
                    "SELECT COUNT(*) AS n, COALESCE(MAX(id), 0) AS m "
                    "FROM _honker_notifications WHERE channel='reverse'"
                )
                jm = db.query("PRAGMA journal_mode")
                dv = db.query("PRAGMA data_version")
                diag(
                    "tick=%d t=%.1fs got=%d count=%d max_id=%s journal_mode=%s data_version=%s"
                    % (
                        i,
                        time.monotonic() - t0,
                        len(got),
                        rows[0]["n"],
                        rows[0]["m"],
                        jm[0]["journal_mode"] if jm else "?",
                        dv[0]["data_version"] if dv else "?",
                    )
                )
            except Exception as exc:
                diag("diag query error: " + repr(exc))

    diag_task = asyncio.create_task(diagnostics())
    try:
        await asyncio.wait_for(consume(), timeout=20.0)
        print("RESULT", json.dumps(got), flush=True)
    finally:
        diag_task.cancel()
        try:
            await diag_task
        except (asyncio.CancelledError, Exception):
            pass

asyncio.run(main())
`;
      proc = spawnPython(pyScript);
      const nextLineMatching = createLineReader(proc.stdout);

      await nextLineMatching((line) => line === 'READY', 25000);

      const tx = db.transaction();
      tx.notify('reverse', { tag: 'a', i: 1 });
      tx.notify('reverse', { tag: 'b', i: 2 });
      tx.commit();
      const tx2 = db.transaction();
      tx2.notify('reverse', { tag: 'c', i: 3 });
      tx2.commit();

      const resultLine = await nextLineMatching(
        (line) => line.startsWith('RESULT '),
        25000,
      );
      const result = JSON.parse(resultLine.slice('RESULT '.length));

      await new Promise((resolve) => proc.on('exit', resolve));
      assert.deepEqual(
        result.map((row) => row.i),
        [1, 2, 3],
      );
      assert.deepEqual(
        result.map((row) => row.tag),
        ['a', 'b', 'c'],
      );
    } finally {
      db?.close();
      await stopChild(proc);
      cleanup();
    }
  },
);
