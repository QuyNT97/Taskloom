import test from 'node:test';
import assert from 'node:assert/strict';
import {
  TaskRuntime, PluginKey, definePlugin, TaskCancelledError,
  EngineDestroyedError, TransactionRejectedError,
} from '@yuqgnort/taskloom';

const plugin = (name, spec) => definePlugin({ key: new PluginKey(name), ...spec });
const selector = () => plugin('test-selection', { scheduling: { pickNext: (_, candidates) => candidates[0] } });
const capacity = limit => plugin('test-admission', { scheduling: { canStart: (_, state, context) => context.activeWorkers < limit } });
function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
async function flush() { for (let i = 0; i < 30; i++) await Promise.resolve(); }
function runtime(worker, plugins = [], options = {}) {
  return new TaskRuntime({ worker, plugins: [selector(), ...plugins], ...options });
}
function fakeClock() {
  let time = 0;
  const timers = new Set();
  return {
    now: () => time,
    setTimeout(callback, delay) {
      const entry = { at: time + delay, callback };
      timers.add(entry);
      return () => timers.delete(entry);
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

test('worker execution, status, context and promise inference semantics', async () => {
  const work = deferred();
  const clock = fakeClock();
  clock.advance(17);
  let context;
  const rt = runtime((input, ctx) => { assert.equal(input, 4); context = ctx; return work.promise; }, [], { clock });
  const handle = rt.add(4);
  assert.equal(handle.status, 'pending');
  assert.equal(context, undefined);
  await flush();
  assert.equal(handle.status, 'running');
  assert.equal(context.startedAt, 17);
  assert.equal(context.attempt, 1);
  assert.equal(context.taskId, handle.id);
  assert.equal(context.executionId, rt.state.running.get(handle.id).executionId);
  assert.equal(context.signal.aborted, false);
  work.resolve(8);
  assert.equal(await handle.result, 8);
  assert.equal(handle.status, 'succeeded');
  assert.equal(rt.state.running.size, 0);
  assert.equal(rt.state.pending.length, 0);
  assert.equal(rt.activeWorkers, 0);
  assert.equal(context.signal.aborted, false);
  rt.destroy();
});

test('sync throws and rejected worker promises fail only their own task', async () => {
  const error = new Error('worker error');
  const rt = runtime(input => {
    if (input === 'throw') throw error;
    if (input === 'reject') return Promise.reject(error);
    return 42;
  });
  for (const input of ['throw', 'reject']) {
    const handle = rt.add(input);
    await assert.rejects(handle.result, value => value === error);
    assert.equal(handle.status, 'failed');
  }
  assert.equal(await rt.add('success').result, 42);
  assert.equal(rt.destroyed, false);
  rt.destroy();
});

test('pending cancellation never invokes worker and preserves reason', async () => {
  let calls = 0;
  const rt = runtime(() => { calls++; });
  const handle = rt.add('input');
  const reason = { source: 'user' };
  handle.cancel(reason);
  await assert.rejects(handle.result, error => error instanceof TaskCancelledError && error.reason === reason);
  await flush();
  assert.equal(calls, 0);
  assert.equal(handle.status, 'cancelled');
  assert.equal(rt.state.pending.length, 0);
  handle.cancel();
  rt.cancel('missing');
  rt.destroy();
});

for (const outcome of ['resolve', 'reject']) {
  test(`running cancellation ignores late worker ${outcome}`, async () => {
    const work = deferred();
    let signal;
    const rt = runtime((_, ctx) => { signal = ctx.signal; return work.promise; });
    const handle = rt.add('input');
    await flush();
    rt.cancel(handle.id, 'stop');
    assert.equal(signal.aborted, true);
    assert.equal(signal.reason.reason, 'stop');
    const version = rt.state.version;
    await assert.rejects(handle.result, TaskCancelledError);
    work[outcome](outcome === 'resolve' ? 'late' : new Error('late'));
    await flush();
    assert.equal(rt.state.version, version);
    assert.equal(handle.status, 'cancelled');
    assert.equal(rt.activeWorkers, 0);
    rt.destroy();
  });
}

test('ID reuse and old handle cancellation cannot affect the replacement execution', async () => {
  const first = deferred();
  const second = deferred();
  const contexts = [];
  const rt = runtime((input, ctx) => { contexts.push(ctx); return input === 1 ? first.promise : second.promise; });
  const old = rt.add(1, { id: 'same' });
  await flush();
  old.cancel();
  await assert.rejects(old.result, TaskCancelledError);
  const next = rt.add(2, { id: 'same' });
  await flush();
  assert.notEqual(contexts[0].executionId, contexts[1].executionId);
  old.cancel();
  first.resolve('stale');
  await flush();
  assert.equal(next.status, 'running');
  assert.equal(contexts[1].signal.aborted, false);
  second.resolve('fresh');
  assert.equal(await next.result, 'fresh');
  rt.destroy();
});

test('custom external scheduler controls order and admission composes independently', async () => {
  const order = [];
  const rt = new TaskRuntime({
    worker: input => { order.push(input); return input; },
    plugins: [plugin('reverse', { scheduling: { pickNext: (_, candidates) => candidates.at(-1) } }), capacity(1)],
  });
  const handles = [rt.add('a'), rt.add('b'), rt.add('c')];
  await Promise.all(handles.map(handle => handle.result));
  assert.deepEqual(order, ['c', 'b', 'a']);
  rt.destroy();
});

test('admission counts physically active workers even after AbortSignal is ignored', async () => {
  const jobs = [deferred(), deferred(), deferred()];
  const started = [];
  let peak = 0;
  const rt = runtime(input => {
    started.push(input);
    peak = Math.max(peak, rt.activeWorkers);
    return jobs[input].promise;
  }, [capacity(2)]);
  const handles = [rt.add(0), rt.add(1), rt.add(2)];
  await flush();
  assert.deepEqual(started, [0, 1]);
  handles[0].cancel();
  await assert.rejects(handles[0].result, TaskCancelledError);
  await flush();
  assert.equal(rt.activeWorkers, 2);
  assert.deepEqual(started, [0, 1]);
  jobs[0].resolve('ignored');
  await flush();
  assert.deepEqual(started, [0, 1, 2]);
  assert.equal(peak, 2);
  jobs[1].resolve('one'); jobs[2].resolve('two');
  assert.deepEqual(await Promise.all(handles.slice(1).map(handle => handle.result)), ['one', 'two']);
  rt.destroy();
});

test('a vetoed candidate does not spin or block another task', async () => {
  let checks = 0;
  const rt = runtime(input => input, [plugin('veto-a', {
    filterTransaction(tr) {
      if (!tr.isExecute) return true;
      checks++;
      return !tr.steps.some(step => step.type === 'start' && step.taskId === 'a');
    },
  })]);
  const a = rt.add('a', { id: 'a' });
  const b = rt.add('b', { id: 'b' });
  assert.equal(await b.result, 'b');
  await flush();
  assert.equal(a.status, 'pending');
  assert.ok(checks < 10);
  rt.destroy();
  await assert.rejects(a.result, EngineDestroyedError);
});

test('rejected enqueue returns a failed handle; rejected cancellation preserves running task', async () => {
  const work = deferred();
  let signal;
  const rt = runtime((_, ctx) => { signal = ctx.signal; return work.promise; }, [plugin('veto', {
    filterTransaction: tr => !tr.steps.some(step => step.type === 'cancel' || (step.type === 'enqueue' && step.task.input === 'bad')),
  })]);
  const bad = rt.add('bad');
  await assert.rejects(bad.result, TransactionRejectedError);
  assert.equal(bad.status, 'failed');
  const good = rt.add('good');
  await flush();
  good.cancel();
  assert.equal(signal.aborted, false);
  assert.equal(good.status, 'running');
  work.resolve('done');
  assert.equal(await good.result, 'done');
  rt.destroy();
});

test('appended cancellation prevents start from producing worker side effects', async () => {
  let calls = 0;
  const rt = runtime(() => { calls++; }, [plugin('cancel-start', {
    appendTransaction: (trs, old, next) => {
      const step = trs.flatMap(tr => tr.steps).find(step => step.type === 'start');
      return step ? next.tr.cancel(step.taskId) : null;
    },
  })]);
  const handle = rt.add('a');
  await assert.rejects(handle.result, TaskCancelledError);
  await flush();
  assert.equal(calls, 0);
  rt.destroy();
});

test('external fail/reenqueue keeps one handle and increments attempt without core retry policy', async () => {
  const contexts = [];
  const retryOnce = plugin('test-reenqueue', {
    appendTransaction(trs, old, next) {
      const failed = trs.flatMap(tr => tr.steps).find(step => step.type === 'fail');
      return failed ? next.tr.enqueue(old.running.get(failed.taskId).task) : null;
    },
  });
  const rt = runtime((input, ctx) => {
    contexts.push(ctx);
    if (ctx.attempt === 1) throw new Error('first attempt');
    return input * 2;
  }, [retryOnce]);
  const handle = rt.add(4);
  assert.equal(await handle.result, 8);
  assert.deepEqual(contexts.map(ctx => ctx.attempt), [1, 2]);
  assert.notEqual(contexts[0].executionId, contexts[1].executionId);
  assert.equal(handle.status, 'succeeded');
  rt.destroy();
});

test('cancellation while a requeued task awaits external admission prevents another attempt', async () => {
  const clock = fakeClock();
  let ready = true;
  let calls = 0;
  const gate = plugin('test-delayed-admission', {
    scheduling: { canStart: () => ready },
    appendTransaction(trs, old, next) {
      const failed = trs.flatMap(tr => tr.steps).find(step => step.type === 'fail');
      return failed ? next.tr.enqueue(old.running.get(failed.taskId).task) : null;
    },
    runtime: {
      onTaskStart(ctx) {
        ready = false;
        return ctx.clock.setTimeout(() => { ready = true; ctx.wake(); }, 10);
      },
      onTransaction(ctx) {
        if (ctx.result.transactions.some(tr => tr.steps.some(step => step.type === 'fail'))) {
          ctx.clock.setTimeout(() => { ready = true; ctx.wake(); }, 10);
        }
      },
    },
  });
  const rt = runtime(() => { calls++; throw new Error('fail'); }, [gate], { clock });
  const handle = rt.add('a');
  await flush();
  assert.equal(handle.status, 'pending');
  handle.cancel();
  await assert.rejects(handle.result, TaskCancelledError);
  clock.advance(20);
  await flush();
  assert.equal(calls, 1);
  rt.destroy();
});

test('external runtime timer can fail an execution; cleanup prevents stale timeout', async () => {
  const clock = fakeClock();
  const work = deferred();
  let signal;
  const timeoutError = new Error('external timeout');
  const timerPlugin = plugin('test-timer', { runtime: {
    onTaskStart(ctx) {
      return ctx.clock.setTimeout(() => {
        ctx.dispatch(ctx.state.tr.fail(ctx.taskId, ctx.executionId, timeoutError));
      }, 10);
    },
  } });
  const rt = runtime((_, ctx) => { signal = ctx.signal; return work.promise; }, [timerPlugin], { clock });
  const handle = rt.add('a');
  await flush();
  clock.advance(10);
  await assert.rejects(handle.result, error => error === timeoutError);
  assert.equal(signal.aborted, true);
  assert.equal(clock.size, 0);
  work.resolve('late');
  await flush();
  assert.equal(handle.status, 'failed');
  rt.destroy();

  const quick = runtime(() => 1, [timerPlugin], { clock });
  assert.equal(await quick.add('b').result, 1);
  assert.equal(clock.size, 0);
  clock.advance(20);
  assert.equal(quick.destroyed, false);
  quick.destroy();
});

test('runtime hooks observe commits, can dispatch without recursion and clean up once', async () => {
  const events = [];
  let depth = 0;
  let maxDepth = 0;
  const work = deferred();
  const hook = plugin('hooks', { runtime: {
    setup: () => { events.push('setup'); return () => events.push('destroy'); },
    onTransaction(ctx) {
      depth++; maxDepth = Math.max(maxDepth, depth);
      events.push('commit');
      if (ctx.result.transactions.some(tr => tr.isEnqueue)) ctx.dispatch(ctx.state.tr.setMeta('observed', true));
      depth--;
    },
    onTaskStart: () => { events.push('start'); return () => events.push('end'); },
  } });
  const rt = runtime(() => work.promise, [hook]);
  const handle = rt.add('a');
  await flush();
  assert.equal(maxDepth, 1);
  work.resolve('done');
  assert.equal(await handle.result, 'done');
  await flush();
  rt.destroy(); rt.destroy();
  assert.equal(events.filter(event => event === 'start').length, 1);
  assert.equal(events.filter(event => event === 'end').length, 1);
  assert.equal(events.filter(event => event === 'destroy').length, 1);
  assert.equal(events[0], 'setup');
});

test('cancellation during onTaskStart aborts its signal and immediately cleans returned resource', async () => {
  let workerCalls = 0, cleanups = 0;
  let signal;
  const rt = runtime(() => { workerCalls++; }, [plugin('cancel-before-worker', { runtime: {
    onTaskStart(ctx) {
      signal = ctx.signal;
      ctx.cancel(ctx.taskId);
      return () => cleanups++;
    },
  } })]);
  const handle = rt.add('a');
  await assert.rejects(handle.result, TaskCancelledError);
  assert.equal(workerCalls, 0);
  assert.equal(signal.aborted, true);
  assert.equal(cleanups, 1);
  rt.destroy();
  assert.equal(cleanups, 1);
});

test('destroy rejects pending/running handles, aborts workers and ignores late results', async () => {
  const work = deferred();
  let signal;
  const rt = runtime((_, ctx) => { signal = ctx.signal; return work.promise; }, [capacity(1)]);
  const running = rt.add('a');
  const pending = rt.add('b');
  await flush();
  const snapshot = rt.state;
  rt.destroy();
  await assert.rejects(running.result, EngineDestroyedError);
  await assert.rejects(pending.result, EngineDestroyedError);
  assert.equal(signal.aborted, true);
  assert.equal(running.status, 'cancelled');
  assert.equal(pending.status, 'cancelled');
  assert.throws(() => rt.dispatch(rt.state.tr), EngineDestroyedError);
  assert.throws(() => rt.add('c'), EngineDestroyedError);
  assert.throws(() => rt.cancel('a'), EngineDestroyedError);
  work.resolve('late');
  await flush();
  assert.equal(rt.state, snapshot);
  assert.equal(rt.activeWorkers, 0);
  rt.destroy();
});

test('public dispatch failure is atomic and runtime stays usable', async () => {
  const rt = runtime(input => input);
  const snapshot = rt.state;
  assert.throws(() => rt.dispatch(snapshot.tr.cancel('missing')), /Unknown task/);
  assert.equal(rt.state, snapshot);
  assert.equal(await rt.add(3).result, 3);
  rt.destroy();
});

test('a filtered worker settlement fails closed instead of leaving handles unresolved', async () => {
  const errors = [];
  const rt = runtime(() => 1, [plugin('veto-complete', {
    filterTransaction: tr => !tr.steps.some(step => step.type === 'complete'),
  })], { onError: error => errors.push(error) });
  const handle = rt.add('a');
  await assert.rejects(handle.result, TransactionRejectedError);
  assert.equal(rt.destroyed, true);
  assert.equal(errors.length, 1);
  assert.equal(rt.error, errors[0]);
});

test('scheduler and hook faults reject active handles and release resources', async () => {
  const boom = new Error('hook fault');
  for (const fault of [
    { scheduling: { canStart() { throw boom; } } },
    { runtime: { onTransaction() { throw boom; } } },
    { runtime: { onTaskStart() { throw boom; } } },
  ]) {
    let cleanup = 0;
    const rt = runtime(() => new Promise(() => {}), [
      plugin('resource', { runtime: { setup: () => () => cleanup++ } }),
      plugin('fault', fault),
    ]);
    const handle = rt.add('a');
    await assert.rejects(handle.result, error => error === boom);
    assert.equal(rt.destroyed, true);
    assert.equal(rt.error, boom);
    assert.equal(cleanup, 1);
  }
});

test('setup failure unwinds prior plugins and cleanup errors do not stop teardown', () => {
  const boom = new Error('setup failed');
  const events = [];
  assert.throws(() => runtime(() => 1, [
    plugin('a', { runtime: { setup: () => () => events.push('a') } }),
    plugin('b', { runtime: { setup: () => () => { events.push('b'); throw new Error('cleanup'); } } }),
    plugin('c', { runtime: { setup: () => { throw boom; } } }),
  ]), error => error === boom);
  assert.deepEqual(events, ['b', 'a']);
});

test('one selection provider is required and invalid selection fails before worker invocation', async () => {
  assert.throws(() => new TaskRuntime({ worker: () => 1, plugins: [] }), /exactly one/);
  assert.throws(() => new TaskRuntime({ worker: () => 1, plugins: [selector(), selector()] }), /exactly one/);
  let calls = 0;
  const rt = new TaskRuntime({ worker: () => calls++, plugins: [plugin('bad-selector', {
    scheduling: { pickNext: () => ({ id: 'not-a-candidate', input: 1, createdAt: 0 }) },
  })] });
  await assert.rejects(rt.add(1).result, /supplied candidates/);
  assert.equal(calls, 0);
});

test('pure hooks cannot reenter runtime mutation', async () => {
  let rt;
  rt = runtime(() => 1, [plugin('impure', { scheduling: { canStart() { rt.add('nested'); return true; } } })]);
  await assert.rejects(rt.add('a').result, /pure state or scheduling hooks/);
  assert.equal(rt.destroyed, true);
});

test('a burst of synchronous workers completes without recursive scheduling', async () => {
  const rt = runtime(input => input * 2);
  const handles = Array.from({ length: 200 }, (_, index) => rt.add(index));
  assert.deepEqual(await Promise.all(handles.map(handle => handle.result)), Array.from({ length: 200 }, (_, index) => index * 2));
  assert.equal(rt.state.pending.length, 0);
  assert.equal(rt.state.running.size, 0);
  rt.destroy();
});
