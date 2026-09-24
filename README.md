# Task Engine

An extensible async task execution engine with transactional state and a
plugin-first architecture. Inspired by ProseMirror's architecture; unaffiliated
with ProseMirror.

**Current scope: Phases 1–4 complete.** Immutable state, transaction pipeline,
execution runtime and the six initial policy plugins are implemented. The
`createTaskEngine` facade, subscriptions and final beginner DX belong to Phase 5.

```ts
import { TaskRuntime } from '@task-engine/core';
import { fifo, concurrency, timeout, retry } from '@task-engine/plugins';

type Input = { value: number };
const runtime = new TaskRuntime<Input, number>({
  worker: async (input, ctx) => {
    if (ctx.attempt === 1) throw new Error('Transient failure');
    return input.value * 2;
  },
  plugins: [
    fifo(), concurrency(4), timeout(5000),
    retry({ maxAttempts: 3 }),
  ],
});
const handle = runtime.add({ value: 21 });
const result = await handle.result; // 42
runtime.destroy();
```

TaskRuntime is the advanced API. Explicit input/result generics preserve typing
across plugin composition; automatic beginner inference is a Phase 5 deliverable.
Use exactly one selection policy: FIFO or priority. Other policies compose through
admission, pure transactions and runtime hooks. A cancelled worker that ignores
AbortSignal still occupies physical capacity until its promise settles.

The pure API remains available independently:

```ts
import { TaskState } from '@task-engine/core';

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

Requires Node.js 20+ for development and TypeScript 5.4+ for the published types.
Core uses platform promises, AbortController and timers, with no Node.js imports.

```sh
npm install
npm run typecheck
npm test
```

The monorepo builds `@task-engine/core` and `@task-engine/plugins` with project
references. Built-in policies import only the core package's public API.
