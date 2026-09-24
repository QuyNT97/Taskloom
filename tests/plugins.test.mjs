import test from 'node:test';
import assert from 'node:assert/strict';
import { TaskRuntime, TaskState, PluginKey, definePlugin, TaskCancelledError, EngineDestroyedError } from '@task-engine/core';
import { fifo, concurrency, priority, timeout, TaskTimeoutError, retry, retryKey, latestBy } from '@task-engine/plugins';

function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
async function flush() { for (let i = 0; i < 40; i++) await Promise.resolve(); }
function fakeClock() {
  let time = 0;
  const timers = new Set();
  return {
    now: () => time,
    setTimeout(callback, delay) {
      const timer = { at: time + delay, callback };
      timers.add(timer);
      return () => timers.delete(timer);
    },
    advance(ms) {
      time += ms;
      for (const timer of [...timers].sort((a, b) => a.at - b.at)) {
        if (timer.at <= time && timers.delete(timer)) timer.callback();
      }
    },
    get size() { return timers.size; },
  };
}
const runtime = (worker, plugins = [], options = {}) => new TaskRuntime({ worker, plugins: [fifo(), ...plugins], ...options });
const task = (id, createdAt, meta = {}) => ({ id, createdAt, input: id, meta });

test('FIFO selects oldest creation timestamp, with stable ties', async () => {
  const order = [];
  const rt = runtime(input => { order.push(input); return input; }, [concurrency(1)]);
  rt.dispatch(rt.state.tr.enqueue(task('new', 30)).enqueue(task('old', 10)).enqueue(task('tie', 10)));
  await flush();
  assert.deepEqual(order, ['old', 'tie', 'new']);
  rt.destroy();
});

test('priority selects high values first, breaks ties by age and defaults to zero', async () => {
  const order = [];
  const rt = new TaskRuntime({ worker: input => { order.push(input); }, plugins: [priority(), concurrency(1)] });
  rt.dispatch(rt.state.tr.enqueue(task('low', 0, { priority: -1 })).enqueue(task('high-new', 20, { priority: 3 }))
    .enqueue(task('normal', 0)).enqueue(task('high-old', 10, { priority: 3 })));
  await flush();
  assert.deepEqual(order, ['high-old', 'high-new', 'normal', 'low']);
  rt.destroy();
  const custom = new TaskRuntime({ worker: input => input, plugins: [priority({ getPriority: task => task.input.rank })] });
  assert.deepEqual(await custom.add({ rank: 9 }).result, { rank: 9 });
  custom.destroy();
});

test('invalid policy options and competing selectors fail with useful errors', () => {
  for (const limit of [0, -1, 0.5, Infinity, NaN]) assert.throws(() => concurrency(limit), /positive safe integer/);
  for (const ms of [-1, Infinity, NaN, 2 ** 31]) assert.throws(() => timeout(ms), /timeout must/);
  for (const maxAttempts of [0, -1, 1.5, Infinity]) assert.throws(() => retry({ maxAttempts }), /positive safe integer/);
  for (const backoff of [{ type: 'other', base: 1 }, { type: 'fixed', base: -1 }, { type: 'fixed', base: 1, max: Infinity }]) {
    assert.throws(() => retry({ maxAttempts: 2, backoff }), /Invalid retry backoff/);
  }
  assert.throws(() => runtime(() => 1, [priority()]), /exactly one/);
});

test('invalid priority result fails runtime without invoking worker', async () => {
  let calls = 0;
  const rt = new TaskRuntime({ worker: () => calls++, plugins: [priority({ getPriority: () => NaN })] });
  await assert.rejects(rt.add('a').result, /finite number/);
  assert.equal(calls, 0);
});

test('concurrency retains a slot until a cancelled noncooperative worker settles', async () => {
  const jobs = [deferred(), deferred(), deferred()];
  const starts = [];
  let peak = 0;
  const rt = runtime(input => { starts.push(input); peak = Math.max(peak, rt.activeWorkers); return jobs[input].promise; }, [concurrency(2)]);
  const handles = [rt.add(0), rt.add(1), rt.add(2)];
  await flush();
  assert.deepEqual(starts, [0, 1]);
  handles[0].cancel();
  await assert.rejects(handles[0].result, TaskCancelledError);
  await flush();
  assert.deepEqual(starts, [0, 1]);
  jobs[0].resolve('late');
  await flush();
  assert.deepEqual(starts, [0, 1, 2]);
  assert.equal(peak, 2);
  jobs[1].resolve('one'); jobs[2].resolve('two');
  await Promise.all(handles.slice(1).map(handle => handle.result));
  rt.destroy();
});

test('timeout rejects with typed error, aborts and ignores late success', async () => {
  const clock = fakeClock();
  const work = deferred();
  let signal;
  const rt = runtime((_, ctx) => { signal = ctx.signal; return work.promise; }, [timeout(10)], { clock });
  const handle = rt.add('a');
  await flush();
  clock.advance(9);
  assert.equal(handle.status, 'running');
  clock.advance(1);
  await assert.rejects(handle.result, error => error instanceof TaskTimeoutError && error.taskId === handle.id && error.timeoutMs === 10);
  assert.equal(signal.aborted, true);
  assert.ok(signal.reason instanceof TaskTimeoutError);
  work.resolve('late');
  await flush();
  assert.equal(handle.status, 'failed');
  assert.equal(clock.size, 0);
  rt.destroy();
});

test('timeout timers are removed on success, cancellation and destroy', async () => {
  const clock = fakeClock();
  const work = deferred();
  const rt = runtime(input => input === 'quick' ? 1 : work.promise, [timeout(10)], { clock });
  assert.equal(await rt.add('quick').result, 1);
  assert.equal(clock.size, 0);
  const cancel = rt.add('cancel');
  await flush();
  assert.equal(clock.size, 1);
  cancel.cancel();
  await assert.rejects(cancel.result, TaskCancelledError);
  assert.equal(clock.size, 0);
  const destroyed = rt.add('destroy');
  await flush();
  rt.destroy();
  await assert.rejects(destroyed.result, EngineDestroyedError);
  assert.equal(clock.size, 0);
  work.resolve(1);
  await flush();
});

test('zero-delay retry preserves a single result promise and cleans plugin state', async () => {
  const attempts = [];
  const rt = runtime((input, ctx) => {
    attempts.push(ctx.attempt);
    if (ctx.attempt < 3) throw new Error('transient');
    return input * 2;
  }, [retry({ maxAttempts: 3 })]);
  const handle = rt.add(5);
  assert.equal(await handle.result, 10);
  assert.deepEqual(attempts, [1, 2, 3]);
  assert.equal(handle.status, 'succeeded');
  assert.equal(retryKey.getState(rt.state).size, 0);
  rt.destroy();
});

test('fixed backoff waits between attempts and maxAttempts includes the initial attempt', async () => {
  const clock = fakeClock();
  const times = [];
  const error = new Error('permanent');
  const rt = runtime(() => { times.push(clock.now()); throw error; }, [retry({ maxAttempts: 3, backoff: { type: 'fixed', base: 10 } })], { clock });
  const handle = rt.add('a');
  await flush();
  assert.equal(handle.status, 'pending');
  assert.equal(clock.size, 1);
  clock.advance(9); await flush();
  assert.deepEqual(times, [0]);
  clock.advance(1); await flush();
  assert.deepEqual(times, [0, 10]);
  clock.advance(10); await flush();
  await assert.rejects(handle.result, value => value === error);
  assert.deepEqual(times, [0, 10, 20]);
  assert.equal(clock.size, 0);
  assert.equal(retryKey.getState(rt.state).size, 0);
  rt.destroy();
});

test('exponential retry backoff grows and obeys maximum delay', async () => {
  const clock = fakeClock();
  const times = [];
  const rt = runtime((_, ctx) => { times.push(clock.now()); if (ctx.attempt < 4) throw new Error('retry'); return 'ok'; },
    [retry({ maxAttempts: 4, backoff: { type: 'exponential', base: 10, max: 15 } })], { clock });
  const handle = rt.add('a');
  await flush();
  clock.advance(10); await flush();
  clock.advance(14); await flush();
  assert.deepEqual(times, [0, 10]);
  clock.advance(1); await flush();
  clock.advance(15); await flush();
  assert.equal(await handle.result, 'ok');
  assert.deepEqual(times, [0, 10, 25, 40]);
  rt.destroy();
});

test('shouldRetry receives error/task/attempt and can veto retries', async () => {
  const expected = new Error('nonretryable');
  const seen = [];
  const rt = runtime(() => { throw expected; }, [retry({ maxAttempts: 5, shouldRetry(error, ctx) {
    seen.push([error, ctx.task.input, ctx.attempt]); return false;
  } })]);
  await assert.rejects(rt.add('input').result, error => error === expected);
  assert.deepEqual(seen, [[expected, 'input', 1]]);
  assert.equal(retryKey.getState(rt.state).size, 0);
  rt.destroy();
});

test('maxAttempts one does not schedule backoff; rejected retry append does not leak plugin state', async () => {
  for (const blocked of [false, true]) {
    const clock = fakeClock();
    let count = 0;
    const filter = definePlugin({ key: new PluginKey('block-retry'), filterTransaction: tr => !tr.getMeta(retryKey) });
    const rt = runtime(() => { count++; throw new Error('fail'); }, [retry({ maxAttempts: blocked ? 3 : 1, backoff: { type: 'fixed', base: 10 } }), ...(blocked ? [filter] : [])], { clock });
    await assert.rejects(rt.add('a').result, /fail/);
    await flush();
    assert.equal(count, 1);
    assert.equal(clock.size, 0);
    assert.equal(retryKey.getState(rt.state).size, 0);
    rt.destroy();
  }
});

test('cancel during backoff removes timer and prevents retry', async () => {
  const clock = fakeClock();
  let attempts = 0;
  const rt = runtime(() => { attempts++; throw new Error('transient'); }, [retry({ maxAttempts: 5, backoff: { type: 'fixed', base: 10 } })], { clock });
  const handle = rt.add('a');
  await flush();
  handle.cancel();
  await assert.rejects(handle.result, TaskCancelledError);
  await flush();
  assert.equal(clock.size, 0);
  clock.advance(100); await flush();
  assert.equal(attempts, 1);
  assert.equal(retryKey.getState(rt.state).size, 0);
  rt.destroy();
});

test('retry timers are isolated across runtimes sharing the same plugin instance', async () => {
  const shared = retry({ maxAttempts: 2, backoff: { type: 'fixed', base: 10 } });
  const aClock = fakeClock(), bClock = fakeClock();
  const worker = (_, ctx) => { if (ctx.attempt === 1) throw new Error('retry'); return 2; };
  const a = runtime(worker, [shared], { clock: aClock });
  const b = runtime(worker, [shared], { clock: bClock });
  const aTask = a.add(1, { id: 'same' }), bTask = b.add(1, { id: 'same' });
  await flush();
  a.destroy();
  await assert.rejects(aTask.result, EngineDestroyedError);
  assert.equal(aClock.size, 0);
  assert.equal(bClock.size, 1);
  bClock.advance(10); await flush();
  assert.equal(await bTask.result, 2);
  b.destroy();
});

test('retry generation prevents an old timer from waking a reused task ID', async () => {
  const clock = fakeClock();
  const rt = runtime(() => { throw new Error('retry'); }, [retry({ maxAttempts: 2, backoff: { type: 'fixed', base: 10 } })], { clock });
  const old = rt.add(1, { id: 'same' });
  await flush();
  clock.advance(5);
  old.cancel();
  await assert.rejects(old.result, TaskCancelledError);
  const fresh = rt.add(2, { id: 'same' });
  await flush();
  clock.advance(5); await flush();
  assert.equal(fresh.status, 'pending');
  clock.advance(5); await flush();
  await assert.rejects(fresh.result, /retry/);
  rt.destroy();
});

for (const reverse of [false, true]) {
  test(`timeout and retry compose in ${reverse ? 'reverse' : 'normal'} plugin order`, async () => {
    const clock = fakeClock();
    const attempts = [];
    const policies = [timeout(10), retry({ maxAttempts: 2, backoff: { type: 'fixed', base: 20 } })];
    const rt = runtime((_, ctx) => {
      attempts.push(ctx.attempt);
      if (ctx.attempt === 2) return 'recovered';
      return new Promise((resolve, reject) => ctx.signal.addEventListener('abort', () => reject(ctx.signal.reason), { once: true }));
    }, [concurrency(1), ...(reverse ? policies.reverse() : policies)], { clock });
    const handle = rt.add('a');
    await flush();
    clock.advance(10); await flush();
    assert.equal(handle.status, 'pending');
    clock.advance(19); await flush();
    assert.deepEqual(attempts, [1]);
    clock.advance(1); await flush();
    assert.equal(await handle.result, 'recovered');
    assert.deepEqual(attempts, [1, 2]);
    assert.equal(clock.size, 0);
    rt.destroy();
  });
}

test('latest replaces pending keyed tasks while leaving other and unkeyed tasks alone', async () => {
  const started = [];
  const rt = runtime(input => { started.push(input); return input; }, [latestBy(task => task.meta?.key)]);
  const a = rt.add('a', { meta: { key: 'search' } });
  const b = rt.add('ab', { meta: { key: 'search' } });
  const c = rt.add('abc', { meta: { key: 'search' } });
  const other = rt.add('other', { meta: { key: 'other' } });
  const unkeyed = [rt.add('unkeyed-1'), rt.add('unkeyed-2')];
  await assert.rejects(a.result, error => error instanceof TaskCancelledError && error.reason.replacementId === b.id);
  await assert.rejects(b.result, TaskCancelledError);
  await Promise.all([c, other, ...unkeyed].map(handle => handle.result));
  assert.deepEqual(started, ['abc', 'other', 'unkeyed-1', 'unkeyed-2']);
  rt.destroy();
});

test('latest handles batched enqueues and leaves no completed-history state', () => {
  const state = TaskState.create({ plugins: [latestBy(task => task.meta.key)] });
  const result = state.applyTransaction(state.tr.enqueue(task('a', 0, { key: 1 })).enqueue(task('b', 1, { key: 1 })).enqueue(task('c', 2, { key: 1 })));
  assert.deepEqual(result.state.pending.map(task => task.id), ['c']);
  assert.equal(result.transactions.length, 2);
});

for (const cancelRunning of [false, true]) {
  test(`latest cancelRunning=${cancelRunning} controls existing execution`, async () => {
    const work = deferred();
    let oldSignal;
    const rt = runtime((input, ctx) => { if (input === 'old') { oldSignal = ctx.signal; return work.promise; } return input; },
      [latestBy(task => task.meta.key, { cancelRunning })]);
    const old = rt.add('old', { meta: { key: 'search' } });
    await flush();
    const current = rt.add('current', { meta: { key: 'search' } });
    assert.equal(await current.result, 'current');
    assert.equal(oldSignal.aborted, cancelRunning);
    if (cancelRunning) await assert.rejects(old.result, TaskCancelledError);
    work.resolve('late'); await flush();
    if (!cancelRunning) assert.equal(await old.result, 'late');
    else assert.equal(old.status, 'cancelled');
    rt.destroy();
  });
}

test('latest cancels a delayed retry so stale search work is never restarted', async () => {
  const clock = fakeClock();
  const calls = [];
  const rt = runtime(input => { calls.push(input); if (input === 'old') throw new Error('retry'); return input; },
    [retry({ maxAttempts: 3, backoff: { type: 'fixed', base: 10 } }), latestBy(task => task.meta.key, { cancelRunning: true })], { clock });
  const old = rt.add('old', { meta: { key: 'search' } });
  await flush();
  const current = rt.add('new', { meta: { key: 'search' } });
  await assert.rejects(old.result, TaskCancelledError);
  assert.equal(await current.result, 'new');
  clock.advance(100); await flush();
  assert.deepEqual(calls, ['old', 'new']);
  assert.equal(clock.size, 0);
  rt.destroy();
});

test('timeout transaction failure is contained by the runtime clock guard', async () => {
  const clock = fakeClock();
  const work = deferred();
  const boom = new Error('filter failure');
  const badFilter = definePlugin({ key: new PluginKey('bad'), filterTransaction(tr) {
    if (tr.steps.some(step => step.type === 'fail')) throw boom;
    return true;
  } });
  const rt = runtime(() => work.promise, [timeout(10), badFilter], { clock });
  const handle = rt.add('a');
  await flush();
  assert.doesNotThrow(() => clock.advance(10));
  await assert.rejects(handle.result, error => error === boom);
  assert.equal(rt.destroyed, true);
  assert.equal(rt.error, boom);
  work.resolve('ignored'); await flush();
});

test('retry supports a failed execution within one multi-step transaction', () => {
  let state = TaskState.create({ plugins: [retry({ maxAttempts: 2 })] });
  state = state.apply(state.tr.enqueue(task('a', 0)).start('a', 'first', 0).fail('a', 'first', new Error('first')));
  assert.equal(state.pending.length, 1);
  assert.equal(retryKey.getState(state).get('a').attempt, 1);
  state = state.apply(state.tr.start('a', 'second', 1).fail('a', 'second', new Error('second')));
  assert.equal(state.pending.length, 0);
  assert.equal(retryKey.getState(state).size, 0);
});

test('retry does not put non-plain input or error objects into immutable plugin state', async () => {
  const input = new Date(0);
  const rt = runtime((value, ctx) => {
    assert.equal(value, input);
    if (ctx.attempt === 1) throw new TypeError('first attempt');
    return value.getTime();
  }, [retry({ maxAttempts: 2 })]);
  assert.equal(await rt.add(input).result, 0);
  rt.destroy();
});
