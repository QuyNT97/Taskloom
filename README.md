# Task Engine

[![CI](https://github.com/QuyNT97/Taskloom/actions/workflows/ci.yml/badge.svg)](https://github.com/QuyNT97/Taskloom/actions/workflows/ci.yml)

An extensible async task execution engine with transactional state and a
plugin-first architecture.

```sh
npm install @yuqgnort/taskloom
```

**The v0.1 scope is complete.** Immutable state, the transaction pipeline,
runtime, six policy plugins, subscriptions and the beginner facade are implemented.

```ts
import {
  createTaskEngine, fifo, concurrency, timeout, retry,
} from '@yuqgnort/taskloom';

const engine = createTaskEngine({
  worker: async (input: { value: number }, ctx) => {
    if (ctx.attempt === 1) throw new Error('Transient failure');
    return input.value * 2;
  },
  plugins: [
    fifo(), concurrency(4), timeout(5000),
    retry({ maxAttempts: 3 }),
  ],
});
const handle = engine.add({ value: 21 });
const result = await handle.result; // 42
engine.destroy();
```

The worker parameter and awaited return determine input and result types. FIFO is
added automatically when no selector is supplied. Use priority instead when
required; providing two selectors is an error. Other policies compose through
admission, transactions and runtime hooks. A cancelled worker that ignores
AbortSignal occupies physical capacity until its promise settles.

`addMany()` enqueues atomically. Pause/resume are transactional and let running
work finish. `clear()` cancels the active set in one transaction. `subscribe()`
reports future committed snapshots after the complete filter/reduce/append
pipeline and returns an idempotent unsubscribe function.

The pure API remains available independently:

```ts
import { TaskState } from '@yuqgnort/taskloom';

const state = TaskState.create<number, number>();
const result = state.applyTransaction(
  state.tr.enqueue({ id: 'a', input: 21, createdAt: 0 }),
);
// result.state is published only after filter/reduce/append completes.
// result.transactions includes the accepted root and plugin appends.
```

Transactions are persistent builders; use the returned value. State/plugin data
are immutable. Input and transaction metadata payloads are borrowed and must be
treated as immutable. Runtime destruction retains its last committed state as a
diagnostic snapshot; check destroyed before treating it as live state.

Documentation:

- [Initial spec](docs/initial-spec.md) and [phased roadmap](docs/roadmap.md)
- [Architecture and transaction contract](docs/architecture.md)
- [Writing a state plugin](docs/writing-a-plugin.md)
- [Runtime, cancellation and lifecycle](docs/runtime.md)
- [Built-in plugins and composition](docs/built-in-plugins.md)
- [Engine facade and subscriptions](docs/engine.md)
- [Release checklist](docs/releasing.md)

Requires Node.js 20+ for development and TypeScript 5.4+ for the published types.
Core uses platform promises, AbortController and timers, with no Node.js imports.

```sh
npm install
npm run typecheck
npm test
```

The `@yuqgnort/taskloom` entry exports the facade, advanced kernel and policies.
Internally, policy-free `@yuqgnort/taskloom-kernel` sits below
`@yuqgnort/taskloom-plugins`,
so policies never receive privileged engine access. See the runnable
[basic example](examples/basic.ts) and [custom plugin example](examples/custom-plugin.ts).

Released under the [MIT License](LICENSE).
