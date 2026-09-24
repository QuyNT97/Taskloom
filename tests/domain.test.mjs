import test from 'node:test';
import assert from 'node:assert/strict';
import { TaskState, PluginKey, definePlugin } from '../packages/core/dist/index.js';
const task = (id = 'a') => ({ id, input: 42, createdAt: 0 });

test('persistent builders and snapshots preserve history', () => {
  const initial = TaskState.create();
  const empty = initial.tr;
  const tr = empty.enqueue(task());
  const next = initial.apply(tr);
  assert.equal(empty.steps.length, 0);
  assert.equal(initial.pending.length, 0);
  assert.equal(next.pending[0].id, 'a');
  assert.equal(next.version, 1);
  assert.throws(() => next.pending.push(task('b')), TypeError);
  assert.equal(next.running.set, undefined);
  assert.throws(() => { tr.steps[0].task.id = 'bad'; }, TypeError);
});
test('atomic validation, branch and stale transaction rejection', () => {
  const state = TaskState.create();
  assert.throws(() => state.apply(state.tr.enqueue(task()).enqueue(task())), /Duplicate/);
  assert.equal(state.pending.length, 0);
  const next = state.apply(state.tr.enqueue(task()));
  assert.throws(() => next.apply(state.tr), /snapshot/);
  assert.throws(() => TaskState.create().apply(state.tr), /snapshot/);
});
test('lifecycle and execution identity reject stale completion after cancellation or reuse', () => {
  let state = TaskState.create();
  state = state.apply(state.tr.enqueue(task()).start('a', 'run-1', 1));
  assert.equal(state.running.get('a').executionId, 'run-1');
  assert.throws(() => state.apply(state.tr.complete('a', 'old', 2)), /execution/);
  state = state.apply(state.tr.cancel('a'));
  assert.throws(() => state.apply(state.tr.complete('a', 'run-1', 2)), /execution/);
  state = state.apply(state.tr.enqueue(task()).start('a', 'run-2', 2));
  assert.throws(() => state.apply(state.tr.complete('a', 'run-1', 2)), /execution/);
  state = state.apply(state.tr.complete('a', 'run-2', 2));
  assert.equal(state.running.size, 0);
  assert.equal(state.pending.length, 0);
});
test('failure, removal and invalid transitions', () => {
  let state = TaskState.create();
  assert.throws(() => state.apply(state.tr.start('missing', 'r', 0)), /non-pending/);
  assert.throws(() => state.apply(state.tr.cancel('missing')), /Unknown/);
  state = state.apply(state.tr.enqueue(task()).start('a', 'r', 0).fail('a', 'r', new Error('failed')));
  state = state.apply(state.tr.enqueue(task()).remove('a'));
  assert.equal(state.pending.length, 0);
});
test('external counter plugin, typed-key identity and metadata isolation', () => {
  const key = new PluginKey('counter');
  const other = new PluginKey('counter');
  const counter = definePlugin({ key, state: { init: () => 0, apply: (tr, value) => value + (tr.getMeta(key) ?? 0) } });
  const state = TaskState.create({ plugins: [counter] });
  const tr = state.tr.setMeta(key, 3).setMeta(other, 99).setMeta('source', 'test');
  const next = state.apply(tr);
  assert.equal(key.getState(state), 0);
  assert.equal(key.getState(next), 3);
  assert.equal(other.getState(next), undefined);
  assert.equal(tr.getMeta('source'), 'test');
  assert.throws(() => TaskState.create({ plugins: [counter, counter] }), /Duplicate PluginKey/);
});
test('plugin state is isolated from retained references and other reducers', () => {
  const key = new PluginKey('owned');
  const seed = { counts: new Map([['a', 1]]), nested: { count: 0 } };
  const plugin = definePlugin({ key, state: { init: () => seed, apply: (_, value) => value } });
  const state = TaskState.create({ plugins: [plugin] });
  seed.counts.set('a', 9);
  seed.nested.count = 9;
  assert.equal(key.getState(state).counts.get('a'), 1);
  assert.equal(key.getState(state).counts.set, undefined);
  assert.throws(() => { key.getState(state).nested.count = 9; }, TypeError);
});
test('reducers see updated core and previous plugin values independent of order', () => {
  const a = new PluginKey('a');
  const b = new PluginKey('b');
  const first = definePlugin({ key: a, state: { init: () => 0, apply: () => 10 } });
  const second = definePlugin({ key: b, state: { init: () => 0, apply: (_, value, old, next) => {
    assert.equal(old.pending.length, 0);
    assert.equal(next.pending.length, 1);
    return a.getState(next);
  } } });
  for (const plugins of [[first, second], [second, first]]) {
    const state = TaskState.create({ plugins });
    const next = state.apply(state.tr.enqueue(task()));
    assert.equal(a.getState(next), 10);
    assert.equal(b.getState(next), 0);
  }
});
test('reducer failure leaves original state intact', () => {
  const key = new PluginKey();
  const state = TaskState.create({ plugins: [definePlugin({ key, state: { init: () => 1, apply: () => { throw new Error('boom'); } } })] });
  assert.throws(() => state.apply(state.tr.enqueue(task())), /boom/);
  assert.equal(state.pending.length, 0);
  assert.equal(key.getState(state), 1);
});
test('plugin data rejects mutable resources and cyclic state', () => {
  const cycle = {}; cycle.self = cycle;
  for (const seed of [cycle, new Date(), { callback() {} }, new Map([[{}, 1]])]) {
    const key = new PluginKey();
    assert.throws(() => TaskState.create({ plugins: [definePlugin({ key, state: { init: () => seed, apply: (_, value) => value } })] }), /Plugin state/);
  }
});
test('transaction copies envelope and metadata container at construction', () => {
  const source = { ...task(), meta: { label: 'original' } };
  const state = TaskState.create();
  const tr = state.tr.enqueue(source);
  source.id = 'changed'; source.meta.label = 'changed';
  const next = state.apply(tr);
  assert.equal(next.pending[0].id, 'a');
  assert.equal(next.pending[0].meta.label, 'original');
});
