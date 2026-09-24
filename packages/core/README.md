# @yuqgnort/taskloom

Public entry point for Task Engine v0.1. It exports `createTaskEngine`, immutable
state and transaction APIs, plugin authoring primitives, the advanced runtime,
and the initial built-in policies.

```sh
npm install @yuqgnort/taskloom
```

```ts
import { createTaskEngine, concurrency, retry } from '@yuqgnort/taskloom';

const engine = createTaskEngine({
  worker: async (input: { value: number }) => input.value * 2,
  plugins: [concurrency(4), retry({ maxAttempts: 3 })],
});

const result = await engine.add({ value: 21 }).result;
engine.destroy();
```

See the repository README and documentation for lifecycle and plugin contracts.
