# Built-in plugins (Phase 4)

Policies live in `@task-engine/plugins`, which imports only public core exports.
`@task-engine/core` contains no retry, timeout, priority or FIFO branches.

```ts
import { TaskRuntime } from '@task-engine/core';
import { fifo, concurrency, timeout, retry } from '@task-engine/plugins';

type Input = { value: number };
const runtime = new TaskRuntime<Input, number>({
  worker: async (input, ctx) => {
    if (ctx.attempt === 1) throw new Error('Try again');
    return input.value * 2;
  },
  plugins: [
    fifo(),
    concurrency(4),
    timeout(5000),
    retry({ maxAttempts: 3, backoff: { type: 'exponential', base: 100 } }),
  ],
});
const handle = runtime.add({ value: 21 });
await handle.result; // 42; one promise across attempts
runtime.destroy();
```

This is the advanced runtime API; specify input/result generics once when composing
policies. Its factories then receive those types contextually. The beginner facade
and automatic inference across inline generic plugin factories are Phase 5 work.
Policies are a separate package to preserve a one-way dependency on core.

## Selection: FIFO or priority

`fifo()` selects the oldest `createdAt` among admitted candidates. Equal timestamps
preserve pending insertion order. `priority({ getPriority? })` selects the highest
finite priority, then oldest creation time, then insertion order. Its default
getter reads `task.meta.priority`, defaulting to zero. Invalid priority values fail
the runtime rather than silently producing inconsistent ordering.

Use exactly one selection policy: `fifo()` **or** `priority()`. They both provide
`pickNext`; combining them raises an initialization error. Priority already
includes the chronological tie break. Policies only choose among candidates
admitted by the other plugins.

## Concurrency

`concurrency(limit)` requires a positive safe integer and admits work while
`activeWorkers < limit`. Physical workers count until their promises settle,
including cancelled workers that ignore AbortSignal. Consequently an uncooperative
worker can hold a slot indefinitely. This prevents cancellation/retry from silently
exceeding the limit. Multiple admission plugins combine with logical AND.

Direct advanced `dispatch(tr.start(...))` bypasses automatic selection/admission;
use normal pending-task scheduling when relying on concurrency limits.

## Timeout

`timeout(milliseconds)` installs a timer for each started execution and returns
its cleanup. The timer dispatches fail for that execution ID. Failure aborts the
worker and rejects the handle with `TaskTimeoutError`, unless retry re-enqueues it.
The typed error exposes taskId and timeoutMs. Every completion/cancellation/destroy
path cancels the timer; late worker outcomes cannot overwrite the timeout.

Timeout applies per attempt, not to the logical task's total lifetime. Durations
must be finite and between zero and 2,147,483,647 milliseconds to avoid platform
timer overflow. Zero means a zero-delay timer, not a synchronous failure.

## Retry

`retry({ maxAttempts, backoff?, shouldRetry? })` counts the first execution in
maxAttempts. Without backoff, an allowed retry becomes immediately eligible.
Fixed backoff uses `base`; exponential backoff uses `base * 2^(attempt - 1)` after
a failed attempt. Optional `max` caps either form; all delays are capped at the
platform timer maximum above. There is no jitter in this phase.

`shouldRetry(error, { task, attempt })` is synchronous and pure. It is consulted
only while attempts remain. False preserves the terminal failure; an exception
aborts the transaction pipeline and disposes the runtime through its normal error
path. Every kind of task failure is eligible by default, including timeout.
Cancellation is never retried.

A pure append transaction re-enqueues the failed task and records waiting state.
A scheduling admission predicate blocks waiting tasks. A per-runtime plugin
instance schedules clock wakeups and publishes typed readiness metadata when the
backoff expires. Timers belong to that runtime even if the plugin definition is
shared. Cancellation, replacement and destruction release them. Generation and
attempt checks stop stale timers from waking another logical task with the same ID.

The exported `retryKey` retrieves readonly per-task attempt, generation and delay
state. Terminal tasks are removed even when a retry append is rejected. Input and
error objects remain outside plugin data state, so non-plain worker payloads are
supported. Only one retry plugin can own the key in a runtime.

If a filter rejects the retry append, the original worker failure is terminal.
If a readiness or timeout transaction is vetoed, its runtime callback fails closed
with TransactionRejectedError, rather than leaving an unresolved task waiting.

## Latest by key

`latestBy(getKey, { cancelRunning?: boolean })` appends cancellation of older tasks
sharing the newly enqueued task's key. The last enqueue in a batch wins. Null and
undefined keys opt out; other keys use Map equality. Keys should be stable.

The default replaces pending tasks only. Set cancelRunning to true to also abort
older running executions. Their handles reject with TaskCancelledError whose
reason contains `{ type: 'superseded', replacementId }`. Noncooperative old workers
may still run physically, but their late outcomes cannot affect the replacement.
Different keys stay independent. A delayed retry is pending and can be superseded.

## Composition and verification

Timeout and retry work in either plugin order. Latest can supersede retry waits;
concurrency still accounts for old workers after abort. All state changes go
through the ordinary filter/append pipeline, so user filters can veto policy
proposals. The test suite uses a manual clock and controlled promises to verify
backoff, timer cleanup, late settlements, ID reuse and shared-plugin isolation.
