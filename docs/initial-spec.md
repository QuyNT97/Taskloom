# Project: Extensible Task Engine

Build a TypeScript open-source library for managing asynchronous tasks.

The project should not be designed as a traditional queue library.

The main idea is:

> A small async execution kernel with a transactional state model and a highly extensible plugin architecture inspired by ProseMirror.

The main strength of the library must be extensibility.

Future features should be implementable as plugins without modifying the core engine.

---

# 1. Core philosophy

The architecture should follow these principles:

1. Small core.
2. Immutable state.
3. All state changes happen through transactions.
4. Plugins may own isolated private state.
5. Plugins must not mutate engine internals directly.
6. Runtime side effects must be separated from pure state logic.
7. New behavior should be implemented through plugins.
8. Avoid hardcoding features such as retry, timeout, priority, FIFO, etc. into the core.
9. Beginner API should remain simple.
10. Advanced users should have access to state, transactions, plugin APIs, and runtime hooks.

Mental model:

```text
ProseMirror

EditorState
Transaction
Plugin
PluginKey
EditorView
dispatch()

↓

Task Engine

TaskState
TaskTransaction
TaskPlugin
PluginKey
TaskRuntime
dispatch()
```

The library should feel like a programmable async execution framework rather than just another queue.

---

# 2. High-level architecture

```text
                   TaskEngine
                       |
                    dispatch
                       |
                TaskTransaction
                       |
         +-------------+-------------+
         |                           |
  filterTransaction              apply state
         |                           |
      Plugins                    TaskState
         |                           |
         +------ appendTransaction --+
                       |
                  new transaction
```

Runtime side:

```text
                  TaskRuntime
                      |
       +--------------+--------------+
       |              |              |
    Worker          Timer        AbortSignal
       |              |              |
       +-------- runtime plugins -----+
```

Separate pure state and side effects:

```text
PURE

TaskState
TaskTransaction
Plugin state
Plugin reducers
Transaction filtering

IMPURE

TaskRuntime
Worker execution
Timers
AbortController
Network
Persistence
Logging
Telemetry
```

---

# 3. Package structure

Initially use a monorepo.

Suggested structure:

```text
packages/

  core/
    src/
      state/
      transaction/
      plugin/
      runtime/
      task/
      events/

  plugins/
    fifo/
    concurrency/
    retry/
    timeout/
    priority/
    latest/

  presets/
    network/
    search/

  adapters/
    react/

examples/
tests/
benchmarks/
docs/
```

The first implementation can keep built-in plugins inside one repository while keeping package boundaries clean enough to split later.

---

# 4. Public API goal

Basic usage must be extremely simple:

```ts
import {
  createTaskEngine,
  fifo,
  concurrency,
  retry,
  timeout,
} from '@task-engine/core';

const engine = createTaskEngine({
  worker: async (input, ctx) => {
    return fetch(input.url, {
      signal: ctx.signal,
    });
  },

  plugins: [
    fifo(),
    concurrency(4),
    timeout(5000),

    retry({
      maxAttempts: 3,
    }),
  ],
});
```

Add task:

```ts
const handle = engine.add({
  url: '/api/users',
});

const result = await handle.result;
```

Cancellation:

```ts
handle.cancel();
```

Observe engine:

```ts
const unsubscribe = engine.subscribe(state => {
  console.log(state);
});
```

Advanced:

```ts
engine.dispatch(
  engine.state.tr
    .enqueue(taskA)
    .enqueue(taskB)
    .cancel(taskC.id),
);
```

The common user should never be forced to instantiate:

```ts
new Scheduler()
new Executor()
new QueueState()
```

Those primitives may exist internally or as advanced APIs.

---

# 5. Core Task model

Use a generic task envelope.

```ts
export interface Task<TInput = unknown> {
  id: string;

  input: TInput;

  key?: string;

  priority?: number;

  createdAt: number;

  meta?: Record<string, unknown>;
}
```

Do not make `priority`, `key`, etc. mandatory for core execution.

Plugins can interpret them.

Consider keeping core even more generic:

```ts
export interface Task<TInput = unknown> {
  id: string;
  input: TInput;
  createdAt: number;
  meta: Record<string, unknown>;
}
```

Plugins may store plugin-specific information in plugin state instead of bloating the task object.

Prefer the cleaner architecture if feasible.

---

# 6. Task status

Define core task lifecycle:

```ts
export type TaskStatus =
  | 'pending'
  | 'running'
  | 'succeeded'
  | 'failed'
  | 'cancelled';
```

State representation should avoid storing unnecessary completed tasks forever.

Design completed-history support as a separate plugin or configurable behavior if possible.

---

# 7. TaskState

Create immutable `TaskState`.

Conceptually:

```ts
export class TaskState<TInput = unknown> {
  readonly version: number;

  readonly pending: readonly Task<TInput>[];

  readonly running: ReadonlyMap<string, RunningTask<TInput>>;

  readonly plugins: readonly TaskPlugin[];

  get tr(): TaskTransaction<TInput>;

  apply(
    tr: TaskTransaction<TInput>,
  ): TaskState<TInput>;
}
```

However, avoid exposing implementation-heavy structures if a better internal representation exists.

Important requirements:

* state is immutable
* applying transaction returns a new state
* plugin states are part of engine state
* plugins can retrieve their state using `PluginKey`

---

# 8. TaskTransaction

All state changes must go through transactions.

Example:

```ts
const tr = state.tr
  .enqueue(task)
  .cancel(task.id)
  .setMeta('source', 'api');
```

Suggested operations:

```ts
enqueue(task)

remove(taskId)

start(taskId)

complete(taskId, result)

fail(taskId, error)

cancel(taskId)

setMeta(key, value)

setPluginMeta(pluginKey, value)
```

Do not couple transaction operations directly to policies like retry or timeout.

Those should be plugin-driven.

Example:

```ts
tr.setMeta(retryPluginKey, {
  type: 'retry',
  attempt: 2,
});
```

Transaction should expose enough information for plugins to inspect changes.

Possible shape:

```ts
interface TransactionStep {
  type:
    | 'enqueue'
    | 'remove'
    | 'start'
    | 'complete'
    | 'fail'
    | 'cancel';

  payload: unknown;
}
```

A transaction may contain multiple steps.

---

# 9. PluginKey

Implement a strongly typed plugin key inspired by ProseMirror.

Example:

```ts
const retryPluginKey =
  new PluginKey<RetryState>('retry');
```

Usage:

```ts
const retryState =
  retryPluginKey.getState(engine.state);
```

API:

```ts
export class PluginKey<TState = unknown> {
  constructor(name?: string);

  getState(
    state: TaskState,
  ): TState | undefined;
}
```

Plugin state must not collide across plugins.

---

# 10. Plugin architecture

This is the most important part of the project.

Create a stable plugin contract.

Suggested API:

```ts
export interface TaskPluginSpec<
  TInput = unknown,
  TResult = unknown,
  TPluginState = unknown,
> {
  key?: PluginKey<TPluginState>;

  state?: {
    init(
      config: PluginInitContext<TInput, TResult>,
    ): TPluginState;

    apply(
      tr: TaskTransaction<TInput, TResult>,
      value: TPluginState,
      oldState: TaskState<TInput, TResult>,
      newState: TaskState<TInput, TResult>,
    ): TPluginState;
  };

  filterTransaction?(
    tr: TaskTransaction<TInput, TResult>,
    state: TaskState<TInput, TResult>,
  ): boolean;

  appendTransaction?(
    transactions:
      readonly TaskTransaction<TInput, TResult>[],

    oldState: TaskState<TInput, TResult>,

    newState: TaskState<TInput, TResult>,
  ):
    | TaskTransaction<TInput, TResult>
    | null
    | undefined;

  runtime?: TaskRuntimePluginSpec<
    TInput,
    TResult
  >;
}
```

Factory:

```ts
export function definePlugin<
  TInput,
  TResult,
  TState,
>(
  spec: TaskPluginSpec<
    TInput,
    TResult,
    TState
  >,
): TaskPlugin;
```

Or:

```ts
new TaskPlugin(spec)
```

Prefer whichever produces the cleaner TypeScript DX.

---

# 11. Runtime plugin API

State plugins alone are not enough because task queues involve side effects.

Create runtime hooks separately.

Possible API:

```ts
export interface TaskRuntimePluginSpec<
  TInput,
  TResult,
> {
  setup?(
    ctx: RuntimeSetupContext<
      TInput,
      TResult
    >,
  ): void | (() => void);

  onTaskQueued?(
    ctx: RuntimeTaskContext<TInput>,
  ): void;

  onTaskStart?(
    ctx: RuntimeTaskContext<TInput>,
  ):
    | void
    | (() => void)
    | Promise<void | (() => void)>;

  onTaskSuccess?(
    ctx: RuntimeTaskSuccessContext<
      TInput,
      TResult
    >,
  ): void | Promise<void>;

  onTaskError?(
    ctx: RuntimeTaskErrorContext<TInput>,
  ): void | Promise<void>;

  onTaskCancel?(
    ctx: RuntimeTaskContext<TInput>,
  ): void | Promise<void>;

  destroy?(): void;
}
```

Runtime hooks may perform side effects.

State hooks must remain pure.

---

# 12. Runtime context

Workers receive:

```ts
export interface TaskContext {
  signal: AbortSignal;

  attempt: number;

  startedAt: number;

  taskId: string;
}
```

Worker:

```ts
export type TaskWorker<
  TInput,
  TResult,
> = (
  input: TInput,
  ctx: TaskContext,
) => Promise<TResult>;
```

---

# 13. TaskHandle

`engine.add()` should return a task handle.

```ts
export interface TaskHandle<TResult> {
  readonly id: string;

  readonly result: Promise<TResult>;

  cancel(reason?: unknown): void;

  get status(): TaskStatus;
}
```

Potential future API:

```ts
handle.subscribe(...)
```

Do not implement unless needed in v1.

---

# 14. Engine API

Suggested public interface:

```ts
export interface TaskEngine<
  TInput,
  TResult,
> {
  readonly state:
    TaskState<TInput, TResult>;

  add(
    input: TInput,
    options?: AddTaskOptions,
  ): TaskHandle<TResult>;

  addMany(
    inputs: readonly TInput[],
  ): TaskHandle<TResult>[];

  cancel(
    taskId: string,
    reason?: unknown,
  ): void;

  pause(): void;

  resume(): void;

  clear(): void;

  dispatch(
    tr:
      TaskTransaction<
        TInput,
        TResult
      >,
  ): void;

  subscribe(
    listener:
      (
        state:
          TaskState<
            TInput,
            TResult
          >,
      ) => void,
  ): () => void;

  destroy(): void;
}
```

Pause/resume may initially be implemented by a plugin if that leads to a cleaner architecture.

Avoid putting behavior into core unless it is truly fundamental.

---

# 15. Core scheduling philosophy

Do not hardcode FIFO.

The kernel needs an abstraction allowing plugins to influence which pending task executes next.

Avoid:

```ts
const task = pending.shift();
```

inside the core.

Create a scheduling capability.

Example:

```ts
export interface Scheduler {
  pickNext(
    state: TaskState,
  ): Task | undefined;
}
```

However, make it extensible enough that plugins can modify scheduling.

Possible architecture:

```text
Pending tasks
     |
Scheduler pipeline
     |
next task
     |
Runtime execution
```

FIFO plugin provides default scheduling.

```ts
fifo()
```

Priority plugin:

```ts
priority({
  getPriority(task) {
    return task.priority ?? 0;
  },
})
```

Later:

```ts
latestBy(...)
roundRobin(...)
weightedFair(...)
deadlineScheduler(...)
adaptiveScheduler(...)
```

should not require core changes.

---

# 16. Capability system

Design an internal capability system if it improves plugin extensibility.

Possible capabilities:

```ts
type Capability =
  | 'scheduler'
  | 'clock'
  | 'storage'
  | 'signals'
  | 'logger'
  | 'metrics';
```

Plugins may declare requirements.

Example:

```ts
definePlugin({
  name: 'batching',

  requires: [
    'scheduler',
    'clock',
  ],
});
```

If required capabilities are missing, fail at initialization with a useful error.

Do not over-engineer this in the first implementation.

Design the core so capability support can be added cleanly.

If feasible, implement a minimal version in v1.

---

# 17. Plugin ordering

Avoid silently depending on arbitrary plugin array ordering where possible.

Explore dependency declarations:

```ts
definePlugin({
  name: 'retry',

  after: ['timeout'],

  before: ['telemetry'],
});
```

Engine can topologically sort plugins.

Requirements:

* deterministic ordering
* cycle detection
* useful error messages

Example:

```text
Plugin dependency cycle:

retry
→ timeout
→ retry
```

If this complicates v1 too much, create the architecture and tests so it can be introduced without breaking public API.

---

# 18. Initial built-in plugins

Implement these as plugins, not hardcoded engine features.

## FIFO

```ts
fifo()
```

Choose oldest pending task.

---

## Concurrency

```ts
concurrency(4)
```

Limit number of simultaneously running tasks.

Example:

```text
pending

A B C D E

max = 2

running:
A B

A finishes

running:
C B
```

---

## Timeout

```ts
timeout(5000)
```

Abort a task after timeout.

Use `AbortController`.

Timeout should produce a typed error.

```ts
class TaskTimeoutError extends Error {}
```

---

## Retry

```ts
retry({
  maxAttempts: 3,

  backoff: {
    type: 'exponential',
    base: 500,
  },

  shouldRetry(error, ctx) {
    return true;
  },
})
```

Retry plugin should be implemented using transactions and/or runtime lifecycle, not special cases in core.

Support:

```text
fixed
exponential
```

Optionally support jitter.

---

## Priority

```ts
priority({
  getPriority(task) {
    return task.meta.priority ?? 0;
  },
})
```

---

## Latest

Support replacing stale tasks.

Example:

```ts
latestBy(task => task.meta.key)
```

For:

```text
search "a"
search "ab"
search "abc"
```

Expected:

```text
a    cancelled / superseded
ab   cancelled / superseded
abc  executed
```

Whether currently running previous task should be cancelled should be configurable.

Example:

```ts
latestBy(
  task => task.meta.key,
  {
    cancelRunning: true,
  },
);
```

---

# 19. Future plugins the architecture must support

Do NOT implement all of these now.

The design must make them possible without core modification.

```text
dedupe
debounce
throttle
rate-limit
batching
circuit-breaker
adaptive-concurrency
round-robin
weighted-fair-queue
deadline scheduling
persistent queue
IndexedDB storage
SQLite storage
OpenTelemetry
metrics
logger
React integration
request coalescing
resource locking
dependency graph
task groups
task pipelines
```

Use these future plugins as architecture tests.

For every major core abstraction, ask:

> Could an external package implement these without editing core?

---

# 20. Plugin example: circuit breaker

The following plugin should theoretically be possible:

```ts
const circuitBreakerKey =
  new PluginKey<CircuitState>(
    'circuit-breaker',
  );

const circuitBreaker =
  definePlugin({
    key: circuitBreakerKey,

    state: {
      init() {
        return {
          failures: 0,
          status: 'closed',
        };
      },

      apply(
        tr,
        value,
      ) {
        const event =
          tr.getMeta(
            circuitBreakerKey,
          );

        if (!event) {
          return value;
        }

        switch (event.type) {
          case 'failure':
            return {
              ...value,
              failures:
                value.failures + 1,
            };

          default:
            return value;
        }
      },
    },

    filterTransaction(
      tr,
      state,
    ) {
      const circuit =
        circuitBreakerKey
          .getState(state);

      if (
        tr.isExecute &&
        circuit?.status ===
          'open'
      ) {
        return false;
      }

      return true;
    },
  });
```

The core must not know what a circuit breaker is.

---

# 21. Plugin example: adaptive concurrency

A future third-party package should be able to implement:

```ts
adaptiveConcurrency({
  min: 1,
  max: 20,

  adjust({
    latency,
    errorRate,
    current,
  }) {
    // arbitrary algorithm
  },
});
```

The plugin can dynamically modify scheduling behavior.

No core modification should be required.

---

# 22. Plugin private state

Each plugin may maintain isolated state.

Example retry state:

```ts
interface RetryState {
  attempts:
    ReadonlyMap<
      string,
      number
    >;
}
```

Access:

```ts
retryPluginKey.getState(
  engine.state,
);
```

Plugins must not directly mutate each other's state.

Cross-plugin communication should happen through:

* transactions
* transaction meta
* capabilities
* explicit plugin APIs

Avoid implicit shared mutable objects.

---

# 23. Transaction meta

Support metadata similar to ProseMirror.

Example:

```ts
tr.setMeta(
  retryPluginKey,
  {
    type: 'retry',
    attempt: 2,
  },
);
```

Retrieve:

```ts
tr.getMeta(
  retryPluginKey,
);
```

Also support non-plugin keys where useful.

Prefer strongly typed plugin-key metadata where possible.

---

# 24. appendTransaction

Implement `appendTransaction`.

This is one of the key extension mechanisms.

Flow:

```text
dispatch transaction
      |
filter plugins
      |
apply state
      |
plugins inspect result
      |
appendTransaction
      |
new transaction?
      |
apply again
```

Prevent infinite append loops.

Use safeguards such as:

* only provide transactions not already seen
* maximum append cycles
* detect transaction identity/version

Follow the conceptual behavior of ProseMirror without copying implementation blindly.

---

# 25. filterTransaction

Plugins can reject transactions.

Example dedupe:

```ts
filterTransaction(
  tr,
  state,
) {
  if (!tr.isEnqueue) {
    return true;
  }

  return !isDuplicate(
    tr,
    state,
  );
}
```

Filtering must be deterministic and synchronous unless there is a very strong architectural reason otherwise.

Keep pure-state transaction processing synchronous.

---

# 26. Runtime lifecycle

Runtime executes pending tasks selected by scheduling policy.

Conceptual loop:

```text
state changes
    |
runtime wakes up
    |
check capacity
    |
select next task
    |
dispatch start transaction
    |
execute worker
    |
success/error
    |
dispatch result transaction
    |
repeat
```

Avoid uncontrolled recursive scheduling.

Use a deterministic scheduler loop.

---

# 27. Cancellation

Cancellation must exist in v1.

Requirements:

```ts
handle.cancel()
```

and:

```ts
engine.cancel(taskId)
```

Pending task:

```text
pending
→ cancelled
```

Running task:

```text
running
→ AbortController.abort()
→ cancelled
```

Worker receives:

```ts
ctx.signal
```

The engine must correctly handle workers that ignore AbortSignal.

Late results from cancelled tasks must not incorrectly transition state to success.

Use execution identity/generation if necessary.

---

# 28. Race safety

The runtime must be resilient to:

* cancelled task resolving later
* retry from stale execution
* same task being superseded
* plugin-generated transactions racing with worker completion
* engine destroy while tasks run

Use a runtime execution token.

Example:

```ts
interface Execution {
  executionId: string;
  taskId: string;
  controller:
    AbortController;
}
```

Only the currently active execution may commit its result.

---

# 29. Event subscription

Provide observable state changes:

```ts
engine.subscribe(state => {
  // render / inspect / metrics
});
```

Potential event API:

```ts
engine.on(
  'task:start',
  ...
);
```

Do not add event APIs if state subscription and runtime plugin hooks already solve the problem.

Avoid redundant APIs.

Prefer one coherent model.

---

# 30. Dynamic plugin configuration

Architect the system so runtime plugin updates may be possible later.

Future API might look like:

```ts
engine.reconfigure({
  plugins: [
    concurrency(8),
    retry({
      maxAttempts: 5,
    }),
  ],
});
```

or:

```ts
engine.use(plugin);

engine.removePlugin(
  pluginKey,
);
```

Do NOT implement dynamic plugin replacement unless it can be done cleanly in v1.

But avoid designs that make it impossible later.

---

# 31. Presets

Presets are composed plugin collections.

Example:

```ts
networkPreset({
  concurrency: 6,
  timeout: 5000,
  retry: 3,
});
```

Internally:

```ts
function networkPreset() {
  return [
    fifo(),
    concurrency(6),
    timeout(5000),
    retry(...),
  ];
}
```

Search preset:

```ts
searchPreset({
  debounce: 200,
  timeout: 3000,
});
```

Possible composition:

```text
latest
debounce
abort previous
timeout
```

Presets should not have privileged engine access.

They are just plugin composition.

---

# 32. No framework dependency

Core must not depend on:

```text
React
Vue
Redux
Zustand
RxJS
fetch
Axios
Node.js-specific runtime
```

Core should ideally work in:

```text
browser
Node.js
Electron
Web Worker
Service Worker
React
non-React applications
```

Use platform APIs carefully.

For timing, consider injecting a clock abstraction where appropriate.

---

# 33. TypeScript quality

The library must have excellent TypeScript DX.

Example inference:

```ts
const engine =
  createTaskEngine({
    worker: async (
      input: {
        userId: string;
      },
    ) => {
      return {
        name: 'John',
      };
    },
  });
```

Should infer:

```ts
TaskEngine<
  { userId: string },
  { name: string }
>
```

Then:

```ts
const handle =
  engine.add({
    userId: '1',
  });

const result =
  await handle.result;

// result.name inferred as string
```

Avoid forcing users to provide redundant generics.

---

# 34. Errors

Create typed errors where useful.

Examples:

```ts
TaskCancelledError

TaskTimeoutError

PluginDependencyError

PluginCycleError

EngineDestroyedError
```

Do not create unnecessary custom errors for every condition.

---

# 35. Dev mode invariants

In development builds, detect invalid plugin behavior.

Examples:

```text
duplicate PluginKey

plugin dependency cycle

dispatch after destroy

complete unknown task

complete cancelled task

same task ID enqueued twice
```

Use meaningful messages.

---

# 36. Testing strategy

Tests are extremely important.

Use unit tests for:

```text
TaskState

TaskTransaction

PluginKey

filterTransaction

appendTransaction

plugin state

scheduler selection

runtime execution

cancellation

retry

timeout

concurrency

race conditions
```

Important race tests:

```text
cancel running task
→ worker resolves later
→ task must remain cancelled
```

```text
task fails
→ retry scheduled
→ task cancelled before retry
→ retry must not execute
```

```text
latest task replaces previous task
→ old worker completes
→ old result ignored
```

```text
concurrency = 2
→ never exceed 2 active workers
```

Use fake timers where useful.

---

# 37. Architecture tests

Write tests proving external plugins can implement new behavior without changing core.

Create at least two test-only custom plugins.

Example:

```text
custom dedupe plugin

custom task counter plugin
```

Possible third:

```text
custom scheduler
```

These tests are specifically intended to validate extensibility.

---

# 38. Documentation

Write documentation for:

```text
Getting Started

Core Concepts

Task

TaskState

Transaction

Plugins

Plugin State

Runtime Plugins

Cancellation

Writing a Plugin

Scheduling

Built-in Plugins

Architecture
```

"Writing a Plugin" is especially important.

Example:

```ts
const logger =
  definePlugin({
    key:
      new PluginKey(
        'logger',
      ),

    runtime: {
      onTaskStart(ctx) {
        console.log(
          'start',
          ctx.task.id,
        );
      },

      onTaskSuccess(ctx) {
        console.log(
          'success',
          ctx.task.id,
        );
      },
    },
  });
```

---

# 39. README positioning

Do not position it as:

> Another JavaScript queue library.

Position it as:

> An extensible async task execution engine with transactional state and a plugin-first architecture.

Potential tagline:

> Async task orchestration where behavior is composed, not hard-coded.

Or:

> A programmable async execution kernel for JavaScript and TypeScript.

Mention inspiration from ProseMirror's architecture, but do not imply affiliation.

---

# 40. Initial implementation scope

Implement v0.1 with:

```text
Task

TaskState

TaskTransaction

Plugin

PluginKey

plugin state

filterTransaction

appendTransaction

TaskRuntime

createTaskEngine

TaskHandle

AbortSignal cancellation

FIFO plugin

Concurrency plugin

Timeout plugin

Retry plugin

Priority plugin

Latest plugin

subscriptions

tests

README
```

Do not implement persistence, React adapter, batching, circuit breaker, or rate limiting yet.

---

# 41. Design constraints

Do not put this in core:

```ts
if (retryEnabled) {}

if (timeoutEnabled) {}

if (priorityEnabled) {}

if (fifoEnabled) {}
```

Instead:

```text
core understands primitives

plugins implement policies
```

Core may understand concepts such as:

```text
task

transaction

state

plugin

runtime

execution

scheduling capability
```

Core must not understand business policies.

---

# 42. Most important architectural question

Whenever implementing a feature, ask:

> Can this behavior be implemented by a third-party plugin without modifying the engine core?

If not, inspect whether the missing primitive belongs in core.

Do not add the feature itself to core.

Add the smallest generic primitive that allows the feature to exist.

This is the most important design rule of the entire project.

---

# 43. Implementation order

Implement in this order.

## Phase 1

Core domain:

```text
Task
TaskState
TaskTransaction
PluginKey
Plugin
plugin state
```

No worker execution yet.

Tests must pass.

---

## Phase 2

Transactions:

```text
filterTransaction

apply

appendTransaction

dispatch pipeline
```

Test plugin extensibility.

---

## Phase 3

Runtime:

```text
worker

scheduler loop

running executions

AbortController

TaskHandle
```

---

## Phase 4

Plugins:

```text
FIFO

Concurrency

Timeout

Retry

Priority

Latest
```

---

## Phase 5

DX:

```text
createTaskEngine

type inference

errors

subscriptions

README

examples
```

---

# 44. Code quality

Requirements:

* TypeScript strict mode.
* Avoid `any`.
* Avoid hidden mutable global state.
* Prefer small modules.
* Public API should be explicitly exported.
* Internal APIs should remain internal.
* Document architectural decisions.
* Add comments for non-obvious runtime/race logic.
* Prefer composition over inheritance.
* Avoid class-heavy design unless a class has clear value.
* Avoid unnecessary abstractions before they are needed.
* Keep engine core understandable.

---

# 45. Deliverables

Produce:

```text
working monorepo

core package

built-in plugins

unit tests

race-condition tests

examples

README

architecture documentation
```

Also produce:

```text
docs/architecture.md
```

that explains:

```text
why transactions exist

why plugins own state

why runtime is separated from state

how scheduling is extensible

how third-party plugins should work

what belongs in core vs plugin
```

---

# 46. Final goal

The finished architecture should allow code like:

```ts
const engine =
  createTaskEngine({
    worker,

    plugins: [
      fifo(),

      concurrency(4),

      timeout(5000),

      retry({
        maxAttempts: 3,
      }),
    ],
  });

const handle =
  engine.add(data);

const result =
  await handle.result;
```

while also allowing an advanced third-party developer to create:

```ts
import {
  definePlugin,
  PluginKey,
} from '@task-engine/core';

export const
  adaptiveConcurrency =
    definePlugin({
      // completely external
      // custom behavior
      // no core modification
    });
```

The engine core should continue working with plugins that were not imagined when the engine was originally created.

That is the primary success criterion.
