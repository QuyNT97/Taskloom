# Architecture — Phases 1–4

Status: domain, transaction pipeline, execution runtime and built-in policies implemented.
The original requirements live in [initial-spec.md](initial-spec.md).

## Transactions and snapshots

`TaskState.create()` creates an empty immutable domain snapshot. `state.tr`
creates a persistent builder: every operation returns a new transaction. Retain
or chain the returned value. `state.apply(tr)` runs the pure, atomic transaction
pipeline; `state.applyTransaction(tr)` also returns the accepted transaction log.
The transaction must reference that exact state object, not merely
its version. Sibling snapshots therefore cannot accidentally accept each other's
transactions. Failed validation or a thrown reducer does not commit a new state.
Each accepted transaction, including a metadata-only or empty transaction,
increments version. One dispatch may therefore increment version more than once.
A rejected root preserves state identity and version.

Task envelopes, metadata records, steps, arrays and running records are copied
and frozen. Readonly maps are facades with no mutation methods. Input, result,
error, cancellation reason and transaction metadata *values* are opaque borrowed
payloads: callers must treat them as immutable. Core does not freeze user-owned
objects such as Responses, Errors or application models. This is structural
immutability, not a deep clone of arbitrary JavaScript objects.

Only active tasks are retained. Complete, fail, cancel and remove release the
active record; transaction steps expose the terminal information to plugins.
A history plugin can retain selected data. Task handles and their terminal status
belong to the runtime, not an unbounded core history map.

Start requires an explicit execution ID and timestamp. Complete/fail require the
matching ID. No clocks, random IDs or workers run during reduction. Invalid or
stale transitions throw, including cancellation of an unknown task. The runtime
must discard late worker settlements before constructing a transaction and must
allocate a fresh execution ID for every execution, including reused task IDs.

## Plugin state and ownership

Plugin keys use object identity; names are diagnostic only. Two equally named
keys are distinct; registering the same key twice fails initialization. Metadata
has a separate type parameter: `PluginKey<State, Meta>`.

`definePlugin` captures callbacks and erases heterogeneous state types only at
the plugin collection boundary. Initialization sees an empty core snapshot with
no initialized plugin values. Reducers all see updated core fields and previous
plugin values in `newState`; no reducer sees another reducer's in-progress result.
This deliberately avoids implicit array-order dependencies. Cross-plugin
communication uses transactions and typed metadata. Reducers must be pure.

Plugin state is owned data, recursively snapshotted and frozen on init/apply.
Supported values: primitives, plain objects, arrays, and maps with primitive
keys (exposed as ReadonlyMap). Cycles, accessors, functions, class instances and
object map keys are rejected. Use runtime-local storage for controllers, timers,
connections and other mutable resources. Copying plugin state is intentionally
conservative in this phase; structural sharing optimization needs benchmarks.

## Transaction pipeline (Phase 2)

`state.applyTransaction(root, options?)` is the pure dispatch pipeline. It returns
an immutable `{ state, transactions }`. `state.apply(root, options?)` returns only
that final state. The single-transaction reducer is private, so callers cannot
accidentally bypass filters. This extends Phase 1's public `apply` behavior;
applications without transaction hooks behave as before. `TaskRuntime.dispatch` publishes the final result atomically; the beginner facade
is reserved for Phase 5.

Processing follows these rules:

1. Validate the append budget and the root's exact originating snapshot.
2. Run filters in configured plugin-array order, short-circuiting on `false`.
   A filter veto rejects the entire transaction, including all steps and metadata.
   A rejected root returns the original state and an empty transaction list;
   reducers and appenders do not run. Domain-step validation follows filtering.
3. Reduce accepted core steps, then plugin state. Reducer `newState` still exposes
   previous plugin values as specified above. Later filters and appenders see
   the fully reduced state, including the updated plugin values.
4. Visit appenders in plugin-array order. Each gets a frozen batch of accepted
   transactions it has not seen, the snapshot immediately before that batch,
   and the current snapshot after that batch. On the first call, `oldState` is
   the state before the root. Hooks must return synchronously.
5. An appender returns `null`/`undefined`, or a transaction built from `newState.tr`.
   Validate snapshot identity before running every filter, including the origin
   plugin's filter. There is no privileged bypass for a plugin's own proposals.
   Accepted appends reduce immediately and enter the log in application order.
6. Advance the appender's cursor even if it returned nothing or its proposal was
   rejected. Its own accepted append is implicitly seen; its next `oldState`
   includes that append. It is never called solely for its own output. Other
   appenders receive that output normally. Rejected proposals enter neither
   state nor the accepted log and never wake other appenders.
7. Repeat passes while new transactions were accepted, calling a hook only when
   it has unseen work. Cursor state is local to one dispatch. An appender should
   express its complete immediate response in one multi-step transaction.

Ordering is explicitly deterministic array order in this phase, not dependency
sorting. Reducers have no in-progress cross-plugin visibility, but filters and
appenders can be order-sensitive by design. Before/after dependency declarations
remain deferred. Plugins must use explicit metadata protocols instead of relying
on unnamed plugins occupying a particular array position.

`maxAppendedTransactions` defaults to 100, excludes the root, and must be a
non-negative safe integer. A zero budget allows roots and vetoed appends but
throws on the first acceptable append. An exact-budget chain that converges is
valid. Attempting the next acceptable append throws `AppendTransactionLimitError`
with `limit` and `pluginName`. This bounds mutually triggering plugins, including
metadata-only or empty transactions. Rejected proposals do not consume the
budget and cannot independently keep the loop alive. Hooks themselves must
terminate; the pipeline cannot interrupt a synchronous callback that never returns.

Any thrown filter/reducer/appender error, stale transaction, invalid accepted
step or limit violation aborts the whole dispatch. There is no returned partial
result; the caller should publish state only after a successful return. Plugin
hooks may see speculative intermediate snapshots, so all three hooks must remain
pure. This cannot roll back external side effects performed by an invalid plugin
or mutation of borrowed payloads. Synchronous return shapes are checked for
JavaScript callers as well as enforced through TypeScript.

Architecture tests define dedupe and task-counter plugins outside core and
import only `@task-engine/core`. They cover atomic batch rejection, running-task
dedupe, appended duplicate rejection, and accepted-only state updates. Pipeline
tests cover precise unseen batches, cursor snapshots, metadata-only appends,
origin filtering, stale snapshots, exception atomicity and loop budgets.

See [Writing a Plugin](writing-a-plugin.md). Runtime hooks are documented in [Runtime](runtime.md). Dependency ordering
is still deferred.

## Runtime and extensibility (Phase 3)

TaskRuntime owns workers, AbortControllers, execution identity and coalesced
scheduling wakes. It consumes the full transaction pipeline before publishing
state or settling handles. State reduction remains free of platform APIs.

Selection and admission are separate plugin capabilities. Physical active-worker
accounting includes aborted workers that have not settled. Generic runtime hooks
provide commit observation, per-execution cleanup and per-runtime setup resources;
clock callbacks can wake scheduling or dispatch transactions. These primitives
have external-plugin tests for selection, admission, timer failure and reenqueue.

See [Runtime](runtime.md) for cancellation, error handling, teardown, publication,
handle continuity and the exact synchronous hook contract. The six [built-in policies](built-in-plugins.md) live in a separate package and
use only public extension points. The beginner facade and subscriptions remain
Phase 5.

Core owns domain transitions, snapshots, execution and generic extension points.
Plugins own scheduling order, concurrency, retries, timeouts, retention and
telemetry. Presets only compose plugins. React and persistence remain deferred.
