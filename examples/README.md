# Node examples

```bash
npm install
npx napi build --platform --release    # if you haven't already
node examples/<name>.js
```

| File | What it shows |
|---|---|
| [`basic.js`](basic.js) | enqueue → claim → ack via `honker_*` SQL functions |
| [`atomic.js`](atomic.js) | `INSERT INTO orders` + enqueue committed in one transaction. Rollback drops both. |
| [`notify_listen.js`](notify_listen.js) | `walEvents()` + `tx.notify()` pub/sub |

The Node binding is a lower-level primitive layer — queue/stream/scheduler operations go through `SELECT honker_*(...)` SQL calls rather than typed JS classes. An idiomatic `Queue` wrapper is on the roadmap. For now, `tx.query("SELECT honker_enqueue(?, ...)")` works cleanly.
