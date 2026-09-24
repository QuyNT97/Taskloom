import test from 'node:test';
import assert from 'node:assert/strict';
import {
  createTaskEngine, fifo, priority, concurrency, retry, timeout, latestBy,
  definePlugin, definePluginFactory, PluginKey, TaskRuntime, TaskState,
  TaskCancelledError, TransactionRejectedError, EngineDestroyedError,
} from '@task-engine/core';
import * as kernel from '@task-engine/kernel';
import * as policies from '@task-engine/plugins';

async function flush() { for (let i = 0; i < 40; i++) await Promise.resolve(); }
function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
const plugin = (name, spec) => definePlugin({ key: new PluginKey(name), ...spec });

test('one public entry exposes the same kernel and plugin identities', () => {
  assert.equal(TaskRuntime, kernel.TaskRuntime);
  assert.equal(TaskState, kernel.TaskState);
  assert.equal(PluginKey, kernel.PluginKey);
  assert.equal(fifo, policies.fifo);
  assert.equal(retry, policies.retry);
  assert.equal(timeout, policies.timeout);
  assert.equal(latestBy, policies.latestBy);
});

test('beginner facade works with only a worker and provides default FIFO', async () => {
  const starts = [];
  const engine = createTaskEngine({ worker: input => { starts.push(input); return input * 2; } });
  assert.ok(Object.isFrozen(engine));
  const handles = engine.addMany([3, 2, 1]);
  assert.deepEqual(await Promise.all(handles.map(handle => handle.result)), [6, 4, 2]);
  assert.deepEqual(starts, [3, 2, 1]);
  assert.equal(engine.state.pending.length, 0);
  assert.equal(engine.destroyed, false);
  engine.destroy();
});

test('custom selection suppresses the default and competing selectors still fail', async () => {
  const order = [];
  const engine = createTaskEngine({
    worker: input => { order.push(input); return input; },
    plugins: [priority(), concurrency(1)],
  });
  const a = engine.add('a', { meta: { priority: 1 } });
  const b = engine.add('b', { meta: { priority: 2 } });
  await Promise.all([a.result, b.result]);
  assert.deepEqual(order, ['b', 'a']);
  engine.destroy();
  assert.throws(() => createTaskEngine({ worker: () => 1, plugins: [fifo(), priority()] }), /exactly one/);
});

test('saved generic policy factories can be shared across worker types and runtimes', async () => {
  const plugins = [fifo(), concurrency(1), retry({ maxAttempts: 2 })];
  const a = createTaskEngine({ worker: (input, ctx) => { if (ctx.attempt === 1) throw new Error('retry'); return input + 1; }, plugins });
  const b = createTaskEngine({ worker: input => input.toUpperCase(), plugins });
  assert.deepEqual(await Promise.all([a.add(1).result, b.add('text').result]), [2, 'TEXT']);
  a.destroy(); b.destroy();
});

test('a factory is instantiated once per engine and ordinary custom plugins still work', async () => {
  let creations = 0;
  const factory = definePluginFactory(() => { creations++; return plugin('external', {}); });
  const counterKey = new PluginKey('counter');
  const counter = definePlugin({ key: counterKey, state: {
    init: () => 0,
    apply: (tr, value) => value + tr.steps.filter(step => step.type === 'enqueue').length,
  } });
  const engine = createTaskEngine({ worker: input => input, plugins: [factory, counter] });
  assert.equal(await engine.add(7).result, 7);
  assert.equal(counterKey.getState(engine.state), 1);
  assert.equal(creations, 1);
  engine.destroy();
});

test('addMany is one atomic transaction and a filter rejects every handle', async () => {
  const snapshots = [];
  const engine = createTaskEngine({ worker: input => input, plugins: [plugin('veto-bad', {
    filterTransaction: tr => !tr.steps.some(step => step.type === 'enqueue' && step.task.input === 'bad'),
  })] });
  engine.subscribe(state => snapshots.push(state));
  const original = engine.state;
  const rejected = engine.addMany(['good', 'bad', 'also-good']);
  await Promise.all(rejected.map(handle => assert.rejects(handle.result, TransactionRejectedError)));
  assert.equal(engine.state, original);
  assert.deepEqual(snapshots, []);
  const accepted = engine.addMany([1, 2, 3]);
  assert.equal(engine.state.version, original.version + 1);
  assert.equal(engine.state.pending.length, 3);
  assert.deepEqual(await Promise.all(accepted.map(handle => handle.result)), [1, 2, 3]);
  await flush();
  assert.equal(snapshots[0].pending.length, 3);
  engine.destroy();
});

test('addMany validation errors remove provisional records and empty batches do not commit', async () => {
  let calls = 0;
  const engine = createTaskEngine({ worker: input => input, clock: {
    now: () => ++calls === 2 ? NaN : 0,
    setTimeout: () => () => {},
  } });
  const before = engine.state;
  assert.deepEqual(engine.addMany([]), []);
  assert.equal(engine.state, before);
  assert.throws(() => engine.addMany(['a', 'b']), /finite createdAt/);
  assert.equal(engine.state, before);
  assert.equal(await engine.add('fresh', { id: 'task-1' }).result, 'fresh');
  engine.destroy();
});

test('subscriptions receive one final snapshot per pipeline, with no intermediate append state', async () => {
  const snapshots = [];
  const engine = createTaskEngine({ worker: input => input, plugins: [plugin('append', {
    appendTransaction: (trs, old, next) => trs.some(tr => tr.isEnqueue) ? next.tr.setMeta('extra', true) : null,
  })] });
  const unsubscribe = engine.subscribe(state => snapshots.push(state));
  engine.pause();
  const before = engine.state.version;
  const handle = engine.add(1);
  const final = engine.state;
  assert.equal(final.version, before + 2);
  await flush();
  assert.deepEqual(snapshots.map(state => state.version), [before, final.version]);
  assert.equal(snapshots.at(-1), final);
  assert.ok(Object.isFrozen(final));
  unsubscribe();
  engine.resume();
  assert.equal(await handle.result, 1);
  await flush();
  assert.equal(snapshots.length, 2);
  engine.destroy();
});

test('subscription membership is captured at commit, and unsubscribe removes queued delivery', async () => {
  const engine = createTaskEngine({ worker: input => input });
  engine.pause();
  const late = [];
  const lateOff = engine.subscribe(state => late.push(state.version));
  await flush();
  assert.deepEqual(late, []);
  const handle = engine.add('a');
  lateOff();
  const replacement = engine.subscribe(state => late.push(state.version));
  await flush();
  assert.deepEqual(late, []);
  engine.resume();
  assert.equal(await handle.result, 'a');
  await flush();
  assert.ok(late.length > 0);
  replacement(); replacement();
  engine.destroy();
});

test('subscriber dispatch is queued, ordered and non-recursive', async () => {
  let depth = 0, peak = 0, added = false;
  const seen = [];
  const engine = createTaskEngine({ worker: input => input });
  engine.pause();
  engine.subscribe(state => {
    depth++; peak = Math.max(peak, depth);
    seen.push(state.version);
    if (!added) { added = true; engine.add('from listener'); }
    depth--;
  });
  const first = engine.add('first');
  await flush();
  assert.equal(peak, 1);
  assert.equal(engine.state.pending.length, 2);
  assert.deepEqual(seen, [2, 3]);
  engine.clear();
  await assert.rejects(first.result, TaskCancelledError);
  engine.destroy();
});

test('sync and async subscriber failures are isolated and reported without destroying the engine', async () => {
  const errors = [];
  const seen = [];
  const sync = new Error('sync listener'), async = new Error('async listener');
  const engine = createTaskEngine({ worker: input => input, onError: error => errors.push(error) });
  engine.subscribe(() => { throw sync; });
  engine.subscribe(async () => { throw async; });
  engine.subscribe(state => seen.push(state.version));
  assert.equal(await engine.add('ok').result, 'ok');
  await flush();
  assert.equal(engine.destroyed, false);
  assert.ok(seen.length >= 3);
  assert.ok(errors.includes(sync)); assert.ok(errors.includes(async));
  assert.equal(engine.error, async);
  engine.destroy();
});

test('subscriber unsubscribe/destroy during delivery stops later callbacks', async () => {
  const engine = createTaskEngine({ worker: input => input });
  engine.pause();
  let secondCalls = 0;
  engine.subscribe(() => { removeSecond(); engine.destroy(); });
  const removeSecond = engine.subscribe(() => secondCalls++);
  const handle = engine.add('a');
  await assert.rejects(handle.result, EngineDestroyedError);
  await flush();
  assert.equal(secondCalls, 0);
  assert.throws(() => engine.subscribe(() => {}), EngineDestroyedError);
});

test('pause is transactional and idempotent; resume restarts pending work', async () => {
  const started = [];
  const engine = createTaskEngine({ worker: input => { started.push(input); return input; } });
  engine.pause();
  const pausedVersion = engine.state.version;
  engine.pause();
  assert.equal(engine.paused, true);
  assert.equal(engine.state.version, pausedVersion);
  const handle = engine.add('queued');
  await flush();
  assert.deepEqual(started, []);
  const rejected = engine.dispatch(engine.state.tr.start(handle.id, 'manual', 0));
  assert.equal(rejected.transactions.length, 0);
  assert.equal(handle.status, 'pending');
  engine.resume();
  assert.equal(engine.paused, false);
  assert.equal(await handle.result, 'queued');
  const version = engine.state.version;
  engine.resume();
  assert.equal(engine.state.version, version);
  engine.destroy();
});

test('pause lets already running work finish but does not launch its successor', async () => {
  const work = deferred();
  const started = [];
  const engine = createTaskEngine({ worker: input => { started.push(input); return input === 1 ? work.promise : input; }, plugins: [concurrency(1)] });
  const handles = engine.addMany([1, 2]);
  await flush();
  engine.pause();
  work.resolve(1);
  assert.equal(await handles[0].result, 1);
  await flush();
  assert.deepEqual(started, [1]);
  assert.equal(handles[1].status, 'pending');
  engine.resume();
  assert.equal(await handles[1].result, 2);
  engine.destroy();
});

test('clear cancels pending and running tasks atomically, aborts and leaves engine reusable', async () => {
  const work = deferred();
  let signal;
  const engine = createTaskEngine({ worker: (input, ctx) => { signal = ctx.signal; return input === 'fresh' ? input : work.promise; }, plugins: [concurrency(1)] });
  const handles = engine.addMany(['running', 'pending']);
  await flush();
  const version = engine.state.version;
  engine.clear();
  assert.equal(engine.state.version, version + 1);
  assert.equal(signal.aborted, true);
  assert.equal(engine.state.pending.length, 0);
  assert.equal(engine.state.running.size, 0);
  await Promise.all(handles.map(handle => assert.rejects(handle.result, TaskCancelledError)));
  work.resolve('late'); await flush();
  assert.equal(await engine.add('fresh').result, 'fresh');
  const empty = engine.state;
  engine.clear();
  assert.equal(engine.state, empty);
  engine.destroy();
});

test('clear and pause obey transaction filters', async () => {
  const work = deferred();
  const engine = createTaskEngine({ worker: () => work.promise, plugins: [plugin('veto-controls', {
    filterTransaction: tr => tr.steps.length > 0 && !tr.steps.some(step => step.type === 'cancel'),
  })] });
  engine.pause();
  assert.equal(engine.paused, false);
  const handle = engine.add('a');
  await flush();
  const before = engine.state;
  engine.clear();
  assert.equal(engine.state, before);
  assert.equal(handle.status, 'running');
  work.resolve('ok');
  assert.equal(await handle.result, 'ok');
  engine.destroy();
});

test('facade methods stay bound and reject mutations after destroy', async () => {
  const engine = createTaskEngine({ worker: input => input });
  const { add, addMany, pause, resume, clear, destroy } = engine;
  assert.equal(await add(1).result, 1);
  destroy(); destroy();
  for (const mutate of [() => add(2), () => addMany([]), pause, resume, clear]) {
    assert.throws(mutate, EngineDestroyedError);
  }
});
