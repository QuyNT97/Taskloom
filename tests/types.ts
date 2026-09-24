import { definePlugin, PluginKey, TaskState, TaskRuntime, type ApplyTransactionResult, type TaskHandle, type RuntimeClock } from '../packages/core/src/index.js';
import { fifo, concurrency, timeout, retry, priority, latestBy, retryKey } from '@yuqgnort/taskloom-plugins';
const key = new PluginKey<number, { increment: number }>('counter');
const plugin = definePlugin({ key, state: { init: () => 0, apply: (tr, value) => value + (tr.getMeta(key)?.increment ?? 0) } });
const state = TaskState.create({ plugins: [plugin] });
const count: number | undefined = key.getState(state);
state.tr.setMeta(key, { increment: 1 });
// @ts-expect-error metadata is tied to key
state.tr.setMeta(key, { increment: 'wrong' });
const typed = TaskState.create<{ url: string }, number>();
typed.tr.enqueue({ id: 'a', input: { url: '/' }, createdAt: 0 }).complete('a', 'run', 1);
// @ts-expect-error result type preserved
 typed.tr.complete('a', 'run', 'wrong');
// @ts-expect-error input type preserved
 typed.tr.enqueue({ id: 'a', input: 1, createdAt: 0 });
// @ts-expect-error readonly collection
 typed.running.set('a', {});
void count;

const policy = definePlugin<{ url: string }, number, number, { increment: number }>({
  key,
  filterTransaction(tr, current) {
    const amount: number | undefined = tr.getMeta(key)?.increment;
    const url: string | undefined = current.pending[0]?.input.url;
    void amount;
    void url;
    return true;
  },
  appendTransaction(transactions, oldState, newState) {
    // @ts-expect-error callback batches are readonly
    transactions.push(newState.tr);
    const previous: number | undefined = key.getState(oldState);
    void previous;
    return transactions.some(tr => tr.isEnqueue)
      ? newState.tr.setMeta(key, { increment: 1 })
      : null;
  },
});
const configured = TaskState.create<{ url: string }, number>({ plugins: [policy] });
const applied: ApplyTransactionResult<{ url: string }, number> = configured.applyTransaction(configured.tr);
const input: { url: string } | undefined = applied.state.pending[0]?.input;
// @ts-expect-error output transactions retain the result type
applied.transactions[0]?.complete('a', 'run', 'wrong');
// @ts-expect-error dispatch result is readonly
applied.state = configured;
definePlugin({
  key,
  // @ts-expect-error filters must be synchronous
  filterTransaction: async () => true,
});
definePlugin({
  key,
  // @ts-expect-error appenders must be synchronous
  appendTransaction: async () => null,
});
void input;

const executionPlugin = definePlugin<{ value: number }, { doubled: number }, number>({
  key: new PluginKey<number>('runtime-types'),
  scheduling: {
    pickNext: (state, candidates, context) => {
      const active: number = context.activeWorkers;
      const value: number | undefined = candidates[0]?.input.value;
      void active;
      void value;
      return candidates[0];
    },
    canStart: () => true,
  },
  runtime: {
    setup(ctx) {
      const clock: RuntimeClock = ctx.clock;
      return clock.setTimeout(() => ctx.wake(), 10);
    },
    onTransaction(ctx) {
      const value: number | undefined = ctx.result.state.pending[0]?.input.value;
      void value;
    },
    onTaskStart(ctx) {
      const signal: AbortSignal = ctx.signal;
      const attempt: number = ctx.attempt;
      void signal;
      void attempt;
      // @ts-expect-error worker result type is preserved in runtime dispatch
      ctx.dispatch(ctx.state.tr.complete(ctx.taskId, ctx.executionId, 'wrong'));
      return () => {};
    },
  },
});
const runner = new TaskRuntime({
  worker: (input: { value: number }) => ({ doubled: input.value * 2 }),
  plugins: [executionPlugin],
});
const taskHandle: TaskHandle<{ doubled: number }> = runner.add({ value: 2 });
// @ts-expect-error runtime input inferred from worker
runner.add({ value: 'wrong' });
// @ts-expect-error handle result retains worker result type
const wrongResult: Promise<string> = taskHandle.result;
definePlugin({
  key,
  runtime: {
    // @ts-expect-error runtime setup must be synchronous
    setup: async () => {},
    // @ts-expect-error runtime start must be synchronous
    onTaskStart: async () => {},
    // @ts-expect-error transaction observer must be synchronous
    onTransaction: async () => {},
  },
});
definePlugin({
  key,
  scheduling: {
    // @ts-expect-error scheduling admission must be synchronous
    canStart: async () => true,
    // @ts-expect-error selection must be synchronous
    pickNext: async () => undefined,
  },
});
void wrongResult;

const composed = new TaskRuntime<{ url: string }, { length: number }>({
  worker: (input: { url: string }) => ({ length: input.url.length }),
  plugins: [
    fifo(), concurrency(2), timeout(500),
    retry({ maxAttempts: 3, shouldRetry: (_, ctx) => ctx.task.input.url.length > 0 }),
    latestBy(task => task.input.url, { cancelRunning: true }),
  ],
});
const composedHandle: TaskHandle<{ length: number }> = composed.add({ url: '/users' });
// @ts-expect-error policy composition must preserve input type
composed.add({ url: 1 });
const attempts: number | undefined = retryKey.getState(composed.state)?.get(composedHandle.id)?.attempt;
const prioritized = new TaskRuntime<{ rank: number }, number>({
  worker: (input: { rank: number }) => input.rank,
  plugins: [priority({ getPriority: task => task.input.rank })],
});
const rank: Promise<number> = prioritized.add({ rank: 1 }).result;
void attempts;
void rank;
