# Engine facade

`createTaskEngine` is the main API. It infers task input from the worker's first
parameter and result from its awaited return value. Built-in policy factories are
specialized after that inference, so they cannot widen the result to `unknown`.

```ts
import {
  createTaskEngine, concurrency, retry, timeout,
} from '@yuqgnort/taskloom';

const engine = createTaskEngine({
  worker: async (input: { userId: string }, context) => {
    const response = await fetch(`/users/${input.userId}`, {
      signal: context.signal,
    });
    return response.json() as Promise<{ name: string }>;
  },
  plugins: [
    concurrency(4),
    timeout(5_000),
    retry({ maxAttempts: 3 }),
  ],
});

const user = await engine.add({ userId: '1' }).result;
user.name; // string
engine.destroy();
```

FIFO is added when no plugin supplies `pickNext`. Priority and other custom
selectors suppress that default. Exactly one selector must exist after plugin
composition.

## Batches and controls

`addMany(inputs)` creates handles and enqueues every input in one transaction.
If a filter rejects the transaction, no task is committed and every handle rejects
with `TransactionRejectedError`. An empty input list is a no-op.

Pause and resume are ordinary metadata transactions backed by private plugin
state. While paused, pending tasks stay queued and running tasks may finish. A
state filter also rejects manually dispatched start transactions. These controls
are idempotent and may themselves be vetoed by another plugin.

`clear()` sends one cancellation transaction for all active handles. Running
signals are aborted after the pipeline commits. If a filter rejects clear, task
state, handles and signals remain unchanged. The engine can accept new work after
a successful clear.

## Subscriptions

`subscribe(listener)` observes future committed state; it does not immediately
emit the current snapshot. One dispatch causes at most one notification, even
when append hooks accepted several transactions. The listener receives the final
snapshot after the complete pipeline.

Notifications are deferred and delivered in commit order. Dispatching or adding
inside a listener schedules another delivery without recursive callback nesting.
The returned unsubscribe is idempotent. If it runs after commit but before
delivery, that queued callback is suppressed. A listener added after commit does
not receive the older snapshot.

Synchronous throws and rejected promises from listeners are sent to `onError`.
They do not destroy the engine or block other listeners. `destroy()` removes all
subscriptions and rejects unresolved handles with `EngineDestroyedError`.

## Advanced access

`engine.state` is the current immutable `TaskState`. `engine.dispatch(tr)` returns
the full `ApplyTransactionResult`, including accepted root and appended
transactions. `TaskRuntime` remains available when an integration needs direct
access to wakeups, clocks or physical worker counts.

Custom plugins created with `definePlugin` retain their concrete input/result
contract. Reusable policy packages can expose `definePluginFactory`, which is
instantiated once per engine after the worker types are known.
