import test from 'node:test';
import assert from 'node:assert/strict';
import {
  TaskState, PluginKey, definePlugin, AppendTransactionLimitError,
} from '@task-engine/core';
import { dedupeBy, taskCounter } from './fixtures/external-plugins.mjs';

const task = (id, key = id) => ({ id, input: { key }, createdAt: 0 });
const plugin = (name, spec) => definePlugin({ key: new PluginKey(name), ...spec });
const tag = tr => tr.getMeta('tag');

// Architecture tests: both behaviors live entirely outside the package.
test('external dedupe vetoes a whole batch, counter only sees accepted work', () => {
  const counter = taskCounter();
  let state = TaskState.create({ plugins: [dedupeBy(task => task.input.key), counter.plugin] });
  const rejected = state.applyTransaction(state.tr.enqueue(task('a', 'same')).enqueue(task('b', 'same')));
  assert.equal(rejected.state, state);
  assert.deepEqual(rejected.transactions, []);
  assert.equal(counter.key.getState(state).enqueued, 0);
  state = state.apply(state.tr.enqueue(task('a', 'same')).start('a', 'run', 1));
  assert.equal(state.apply(state.tr.enqueue(task('b', 'same'))), state);
  state = state.apply(state.tr.complete('a', 'run', 42).enqueue(task('b', 'same')));
  assert.equal(counter.key.getState(state).enqueued, 2);
  assert.deepEqual(state.pending.map(task => task.id), ['b']);
});

test('rejected root skips reducers and append hooks; filters short-circuit in order', () => {
  const calls = [];
  const state = TaskState.create({ plugins: [
    plugin('first', { filterTransaction: () => { calls.push('first'); return true; } }),
    plugin('veto', {
      filterTransaction: () => { calls.push('veto'); return false; },
      state: { init: () => 0, apply: () => assert.fail('must not reduce') },
      appendTransaction: () => assert.fail('must not append'),
    }),
    plugin('last', { filterTransaction: () => assert.fail('must short circuit') }),
  ] });
  const result = state.applyTransaction(state.tr.enqueue(task('a')));
  assert.equal(result.state, state);
  assert.deepEqual(result.transactions, []);
  assert.deepEqual(calls, ['first', 'veto']);
  assert.ok(Object.isFrozen(result));
  assert.ok(Object.isFrozen(result.transactions));
});

test('apply also runs filters and appends; reducers see each accepted transaction', () => {
  const counter = taskCounter();
  const state = TaskState.create({ plugins: [counter.plugin, plugin('follow-up', {
    appendTransaction: (transactions, old, next) => transactions.some(tr => tr.isEnqueue)
      ? next.tr.enqueue(task('follow-up')) : null,
  })] });
  const result = state.applyTransaction(state.tr.enqueue(task('root')));
  assert.deepEqual(result.state.pending.map(task => task.id), ['root', 'follow-up']);
  assert.equal(counter.key.getState(result.state).enqueued, 2);
  assert.equal(result.state.version, 2);
  assert.equal(result.transactions.length, 2);
  assert.equal(result.transactions[0].before, state);
  assert.equal(result.transactions[1].before.version, 1);
  assert.equal(counter.key.getState(state).enqueued, 0);
  assert.deepEqual(state.apply(state.tr.enqueue(task('root'))).pending, result.state.pending);
  assert.throws(() => result.transactions.push(state.tr), TypeError);
});

test('append cursors carry precise old/new snapshots, skip own output and freeze batches', () => {
  const calls = { a: [], b: [], c: [] };
  const observer = (name, nextTag) => plugin(name, {
    appendTransaction(transactions, old, next) {
      assert.ok(Object.isFrozen(transactions));
      calls[name].push({ tags: transactions.map(tag), old: old.version, next: next.version });
      assert.equal(transactions[0].before, old);
      const output = nextTag(transactions.map(tag));
      return output ? next.tr.setMeta('tag', output) : null;
    },
  });
  const state = TaskState.create({ plugins: [
    observer('a', tags => tags.includes('root') ? 'a' : tags.includes('b') ? 'a2' : null),
    observer('b', tags => tags.includes('a') ? 'b' : null),
    observer('c', () => null),
  ] });
  const root = state.tr.setMeta('tag', 'root');
  const result = state.applyTransaction(root);
  assert.deepEqual(result.transactions.map(tag), ['root', 'a', 'b', 'a2']);
  assert.deepEqual(calls, {
    a: [{ tags: ['root'], old: 0, next: 1 }, { tags: ['b'], old: 2, next: 3 }],
    b: [{ tags: ['root', 'a'], old: 0, next: 2 }, { tags: ['a2'], old: 3, next: 4 }],
    c: [{ tags: ['root', 'a', 'b'], old: 0, next: 3 }, { tags: ['a2'], old: 3, next: 4 }],
  });
  assert.equal(result.state.version, 4);
  // Replaying the same immutable input produces the same accepted sequence.
  assert.deepEqual(state.applyTransaction(root).transactions.map(tag), result.transactions.map(tag));
});

test('origin filter applies to its own append and rejects it without another notification', () => {
  let appendCalls = 0;
  const filters = [];
  const state = TaskState.create({ plugins: [plugin('self-filter', {
    filterTransaction(tr) { filters.push(tag(tr)); return tag(tr) !== 'blocked'; },
    appendTransaction(transactions, old, next) {
      appendCalls++;
      return next.tr.setMeta('tag', 'blocked');
    },
  })] });
  const result = state.applyTransaction(state.tr.setMeta('tag', 'root'), { maxAppendedTransactions: 0 });
  assert.deepEqual(filters, ['root', 'blocked']);
  assert.equal(appendCalls, 1);
  assert.deepEqual(result.transactions.map(tag), ['root']);
  assert.equal(result.state.version, 1);
});

test('rejected appends are invisible; appenders can wake again on later accepted work', () => {
  const seen = [];
  const state = TaskState.create({ plugins: [
    plugin('proposer', {
      appendTransaction(transactions, old, next) {
        seen.push({ tags: transactions.map(tag), old: old.version, next: next.version });
        return next.tr.setMeta('tag', 'blocked');
      },
    }),
    plugin('veto', { filterTransaction: tr => tag(tr) !== 'blocked' }),
    plugin('producer', { appendTransaction: (_, old, next) => next.tr.setMeta('tag', 'accepted') }),
  ] });
  const result = state.applyTransaction(state.tr.setMeta('tag', 'root'));
  assert.deepEqual(result.transactions.map(tag), ['root', 'accepted']);
  assert.deepEqual(seen, [
    { tags: ['root'], old: 0, next: 1 },
    { tags: ['accepted'], old: 1, next: 2 },
  ]);
});

test('appended transactions are filtered against the current fully reduced plugin state', () => {
  const counter = taskCounter();
  const state = TaskState.create({ plugins: [
    counter.plugin,
    plugin('quota', { filterTransaction: (_, current) => counter.key.getState(current).enqueued < 1 }),
    plugin('proposer', { appendTransaction: (_, old, next) => next.tr.enqueue(task('extra')) }),
  ] });
  const result = state.applyTransaction(state.tr.enqueue(task('root')));
  assert.equal(result.transactions.length, 1);
  assert.equal(counter.key.getState(result.state).enqueued, 1);
});

test('external dedupe also rejects an appended duplicate', () => {
  const counter = taskCounter();
  const state = TaskState.create({ plugins: [
    dedupeBy(task => task.input.key), counter.plugin,
    plugin('proposer', { appendTransaction: (_, old, next) => next.tr.enqueue(task('b', 'same')) }),
  ] });
  const result = state.applyTransaction(state.tr.enqueue(task('a', 'same')));
  assert.equal(result.transactions.length, 1);
  assert.equal(counter.key.getState(result.state).enqueued, 1);
});

test('stale root is rejected before filters, even at an identical version', () => {
  const state = TaskState.create({ plugins: [plugin('veto', { filterTransaction: () => assert.fail('identity check must run first') })] });
  assert.throws(() => state.applyTransaction(TaskState.create().tr), /different state snapshot/);
});

test('stale or reused appended transactions cannot be hidden by a veto', () => {
  for (const stale of ['root', 'old', 'sibling']) {
    let root;
    const state = TaskState.create({ plugins: [plugin('bad-append', {
      filterTransaction: tr => tr === root,
      appendTransaction: (_, old, next) => stale === 'root' ? root : stale === 'old' ? old.tr : TaskState.create().tr,
    })] });
    root = state.tr.enqueue(task('a'));
    assert.throws(() => state.applyTransaction(root), /different state snapshot/);
    assert.equal(state.pending.length, 0);
  }
});

for (const stage of ['filter', 'reducer', 'append', 'invalid-step']) {
  test(`${stage} error during append processing leaves caller state and plugin state unchanged`, () => {
    const counter = taskCounter();
    const boom = new Error('plugin failure');
    const failure = plugin('failure', {
      ...(stage === 'filter' ? { filterTransaction: tr => { if (tag(tr) === 'extra') throw boom; return true; } } : {}),
      ...(stage === 'reducer' ? { state: { init: () => 0, apply: tr => { if (tag(tr) === 'extra') throw boom; return 1; } } } : {}),
      ...(stage === 'append' ? { appendTransaction: () => { throw boom; } } : {}),
    });
    let current = TaskState.create({ plugins: [
      counter.plugin,
      plugin('proposer', { appendTransaction: (_, old, next) => stage === 'invalid-step'
        ? next.tr.start('missing', 'run', 0) : next.tr.enqueue(task('extra')).setMeta('tag', 'extra') }),
      failure,
    ] });
    const original = current;
    assert.throws(() => { current = current.applyTransaction(current.tr.enqueue(task('root'))).state; },
      stage === 'invalid-step' ? /non-pending/ : error => error === boom);
    assert.equal(current, original);
    assert.equal(current.version, 0);
    assert.equal(current.pending.length, 0);
    assert.equal(counter.key.getState(current).enqueued, 0);
  });
}

test('two nonconverging appenders fail with a typed bounded error and no commit', () => {
  const append = (_, old, next) => next.tr;
  let current = TaskState.create({ plugins: [plugin('a', { appendTransaction: append }), plugin('b', { appendTransaction: append })] });
  const original = current;
  assert.throws(() => { current = current.apply(current.tr.enqueue(task('root')), { maxAppendedTransactions: 3 }); }, error => {
    assert.ok(error instanceof AppendTransactionLimitError);
    assert.equal(error.limit, 3);
    assert.equal(error.pluginName, 'b');
    return true;
  });
  assert.equal(current, original);
  assert.throws(() => current.apply(current.tr), error => error instanceof AppendTransactionLimitError && error.limit === 100);
});

test('append budget excludes the root and allows exact-boundary convergence', () => {
  const state = TaskState.create({ plugins: [plugin('one-append', { appendTransaction: (_, old, next) => next.tr })] });
  assert.equal(state.applyTransaction(state.tr, { maxAppendedTransactions: 1 }).transactions.length, 2);
  assert.throws(() => state.apply(state.tr, { maxAppendedTransactions: 0 }), AppendTransactionLimitError);
  const plain = TaskState.create();
  assert.equal(plain.apply(plain.tr, { maxAppendedTransactions: 0 }).version, 1);
  for (const limit of [-1, 0.5, Infinity, NaN, Number.MAX_SAFE_INTEGER + 1]) {
    assert.throws(() => plain.apply(plain.tr, { maxAppendedTransactions: limit }), RangeError);
  }
});

test('JavaScript hooks must return synchronous values of the declared type', () => {
  for (const value of [undefined, 1, Promise.resolve(true)]) {
    const state = TaskState.create({ plugins: [plugin('invalid-filter', { filterTransaction: () => value })] });
    assert.throws(() => state.apply(state.tr), /synchronous boolean: invalid-filter/);
  }
  for (const value of [false, {}, Promise.resolve(null)]) {
    const state = TaskState.create({ plugins: [plugin('invalid-append', { appendTransaction: () => value })] });
    assert.throws(() => state.apply(state.tr), /synchronous TaskTransaction.*invalid-append/);
  }
});

test('null and undefined appends converge; cursor tracking resets on a new dispatch', () => {
  const seen = [];
  const state = TaskState.create({ plugins: [
    plugin('null', { appendTransaction: () => null }),
    plugin('undefined', { appendTransaction: (_, old, next) => { seen.push([old.version, next.version]); } }),
  ] });
  const first = state.applyTransaction(state.tr);
  const second = first.state.applyTransaction(first.state.tr);
  assert.equal(second.transactions.length, 1);
  assert.deepEqual(seen, [[0, 1], [1, 2]]);
});

test('metadata-only append updates typed-key plugin state visible to later appenders', () => {
  const key = new PluginKey('enqueue-count');
  const counter = definePlugin({
    key,
    state: {
      init: () => 0,
      apply: (tr, count) => count + (tr.getMeta(key)?.added ?? 0),
    },
    appendTransaction(transactions, oldState, newState) {
      const added = transactions.reduce((count, tr) => count + tr.steps.filter(step => step.type === 'enqueue').length, 0);
      return added > 0 ? newState.tr.setMeta(key, { added }) : null;
    },
  });
  const observer = plugin('observer', {
    appendTransaction(transactions, old, next) {
      assert.equal(key.getState(old), 0);
      assert.equal(key.getState(next), 2);
      assert.deepEqual(transactions[1].getMeta(key), { added: 2 });
      return null;
    },
  });
  const state = TaskState.create({ plugins: [counter, observer] });
  const result = state.applyTransaction(state.tr.enqueue(task('a')).enqueue(task('b')));
  assert.equal(result.transactions.length, 2);
  assert.equal(key.getState(result.state), 2);
  assert.equal(key.getState(state), 0);
});
