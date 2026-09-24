# Runtime (Phase 3)

`TaskRuntime` is the advanced execution primitive. Most users should begin with
`createTaskEngine`, which adds defaults and subscriptions. The runtime owns state
publication, live worker executions, AbortControllers and handles; policy belongs
to plugins.

```ts
import { TaskRuntime, definePlugin, PluginKey } from '@yuqgnort/taskloom';

const selection = definePlugin<number, number, undefined>({
  key: new PluginKey<undefined>('example-selection'),
  scheduling: { pickNext: (_, candidates) => candidates[0] },
});
const runtime = new TaskRuntime<number, number>({
  worker: async (input: number, ctx) => input * 2,
  plugins: [selection],
});
const handle = runtime.add(21);
await handle.result; // 42
runtime.destroy();
```

## Selection and admission

Exactly one plugin must provide `scheduling.pickNext`. No default FIFO or
concurrency limit is hidden in core. A selector receives the immutable state,
admitted pending candidates, and an `activeWorkers` count. It must return one of
those candidates or undefined. Every `scheduling.canStart` predicate must agree
before a task becomes a candidate. These callbacks are pure and synchronous.

`activeWorkers` includes cancelled workers until their promises actually settle.
An AbortSignal cannot force a noncooperative worker to stop; releasing capacity
immediately would allow physical concurrency to exceed a plugin's limit.

Wakes are coalesced microtasks. Each turn processes a finite batch of committed
notifications and at most one accepted automatic start. Rejected candidates
are skipped for that turn, so a vetoed head cannot starve other candidates or
spin indefinitely. Resource/timer changes can call context `wake()`.

Advanced direct `dispatch(tr.start(...))` explicitly chooses an execution and
bypasses selection/admission. State filters still apply. Automatic starts always
use the scheduling extension. Avoid manual starts if scheduling limits are needed.

## Publication and handles

`dispatch(tr)` runs the complete pure pipeline and returns its result. A rejected
root does not publish state or invoke runtime hooks. Errors from caller-initiated
dispatch propagate synchronously and leave the runtime usable.

`add(input, { id?, meta? })` returns a handle with `id`, `result`, `status` and
`cancel(reason?)`. Generated IDs and execution IDs are local to the runtime.
A rejected enqueue returns a failed handle with `TransactionRejectedError`.
Invalid input/state transactions throw. Handles preserve their terminal status;
core retains only active records. Unobserved handle rejections are internally
handled without changing the original promise's rejection for awaiters.

Use explicit input/result generics on the advanced TaskRuntime when composing
inline policy factories. `createTaskEngine` infers both from its worker. Workers
may return a value, promise or thenable. Context includes taskId, executionId, signal,
attempt and startedAt. Attempt counts committed starts within one logical handle.

Handles settle after all accepted appends. A fail/complete followed by reenqueue
of the same ID in that dispatch continues the same handle. Cancel/remove ends the
handle even when the ID is re-enqueued; the replacement is a new logical task.
An old handle's cancel method cannot affect a replacement using the same ID.

## Cancellation and races

Pending cancellation prevents execution. Running cancellation detaches the
execution before aborting, allowing abort listeners to dispatch safely. Only a
currently active execution object and running record may commit its outcome.
Late fulfillment/rejection from a cancelled, superseded or destroyed execution
is consumed and ignored. Execution-start hooks run only after the full pipeline;
a start cancelled by an append never invokes the worker.

Runtime cancel is idempotent for absent tasks. Filters can veto cancellation;
in that case state, handle and signal stay unchanged. An execution cleanup runs
once on success, failure, cancellation, replacement or destruction.

## Runtime plugins and resources

`runtime.setup(ctx)` runs once per runtime. It can return a cleanup function, or
a per-runtime instance with `onTransaction`, `onTaskStart` and `destroy` callbacks.
Instance hooks override their same-named definition hooks. Use instance closures
for mutable resources when sharing a plugin definition across runtimes.

`onTransaction(ctx)` receives oldState and the exact committed result. Its live
`ctx.state` getter may be newer than `ctx.result.state` because notifications are
deferred. Notifications may dispatch; their effects are queued, not recursive.
They must return undefined. `onTaskStart(ctx)` runs before the worker and may
return a synchronous cleanup function. All runtime hooks in this phase are
synchronous; long async work belongs in workers. State hooks remain pure.

Context exposes a guarded clock (`now`, `setTimeout` returning a timer-cancel
function), dispatch, cancel and wake. Timer callback errors shut down the runtime
and reach onError; plugins must release their own timers through cleanup.

## Errors and destruction

Worker exceptions fail only that task. Automatic pipeline, scheduler or hook
errors reject all active handles, abort workers and dispose the runtime. A vetoed
worker settlement also fails closed with TransactionRejectedError: an already
consumed worker outcome must not leave its promise unresolved. Cleanup errors
are reported independently and do not stop remaining cleanup. Optional onError
and the runtime error getter expose infrastructure errors.

`destroy()` is idempotent, rejects unresolved handles with EngineDestroyedError,
aborts executions and releases resources. It freezes the last committed state
as a diagnostic snapshot, which may still contain active records. Disposal does
not bypass filters to synthesize a new task-state transaction. Check destroyed
before interpreting that snapshot as live state. Later writes throw; late wakes
and worker settlements do nothing. Workers that ignore abort can outlive disposal.
