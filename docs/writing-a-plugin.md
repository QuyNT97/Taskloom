# Writing a state plugin

Plugins import only the package's public exports. Phase 2 supports pure state,
synchronous transaction filtering and synchronous follow-up transactions. Execution hooks and scheduling are available through the separate
[Runtime plugin API](runtime.md).

## Typed state and metadata

The first key type describes stored state; the second describes messages carried
by transactions. Keys use object identity, so export the key if other packages
need to participate in the protocol.

```ts
import { definePlugin, PluginKey, TaskState } from '@task-engine/core';

const countKey = new PluginKey<number, { added: number }>('enqueue-count');
const counter = definePlugin({
  key: countKey,
  state: {
    init: () => 0,
    apply: (tr, count) => count + (tr.getMeta(countKey)?.added ?? 0),
  },
  appendTransaction(transactions, oldState, newState) {
    const added = transactions.reduce(
      (count, tr) => count + tr.steps.filter(step => step.type === 'enqueue').length,
      0,
    );
    return added > 0 ? newState.tr.setMeta(countKey, { added }) : null;
  },
});

let state = TaskState.create({ plugins: [counter] });
const result = state.applyTransaction(state.tr.enqueue({
  id: 'a', input: 'work', createdAt: 0,
}));
state = result.state;
countKey.getState(state); // 1
result.transactions.length; // 2: enqueue, then metadata-only count update
```

This example deliberately uses append to show an explicit metadata protocol. A
simple counter could count enqueue steps directly in its reducer; it does not
need a follow-up transaction.

Appenders receive only unseen accepted transactions. Do not treat their batch
as the entire dispatch history. `oldState` is the snapshot before that batch,
not necessarily before the root. `newState` contains fully reduced plugin data.
Always build proposals from `newState.tr`. The origin will not receive its own
accepted append again, and must not depend on that happening to finish its work.

## Veto a transaction

```ts
const gateKey = new PluginKey<boolean, { blocked: boolean }>('execution-gate');
const gate = definePlugin({
  key: gateKey,
  state: {
    init: () => false,
    apply: (tr, blocked) => tr.getMeta(gateKey)?.blocked ?? blocked,
  },
  filterTransaction(tr, state) {
    return !(tr.isExecute && gateKey.getState(state));
  },
});
```

A veto rejects the whole transaction, not just its start steps. Filters inspect
the pre-transaction state: to unblock and then start, submit two transactions.
All filters also inspect appended transactions, including proposals from their
own plugin. Rejection yields no accepted log entry and no plugin-state update.

## Keep state and effects separate

State must use immutable data: primitives, plain records, arrays, or readonly
maps with primitive keys. Return a fresh value when state changes. Core snapshots
that data so plugins cannot mutate one another's state. Reducers see prior plugin
values in `newState`; their updated values are visible after reduction finishes.

Do not start timers, execute workers, log, publish state, or return promises from
these hooks. Their execution is speculative until the entire pipeline succeeds.
An error or append-loop limit leaves the caller's original state intact, but
cannot undo side effects inside a plugin. Input and metadata payloads are borrowed
and must also be treated as immutable.

See the [pipeline contract](architecture.md#transaction-pipeline-phase-2) for
ordering, cursor behavior and `maxAppendedTransactions`.
