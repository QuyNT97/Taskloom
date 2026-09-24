import {
  createTaskEngine, fifo, concurrency, retry, timeout, latestBy, priority,
  definePlugin, definePluginFactory, PluginKey,
  type TaskEngine, type TaskHandle, type TaskContext, type TaskEngineOptions, type Task,
} from '@task-engine/core';

const simple = createTaskEngine({
  worker: async (input: { userId: string }, ctx) => {
    const signal: AbortSignal = ctx.signal;
    void signal;
    return { name: input.userId };
  },
});
const name: Promise<{ name: string }> = simple.add({ userId: '1' }).result;
// @ts-expect-error worker defines input shape
simple.add({ userId: 1 });
// @ts-expect-error result does not collapse to unknown or any
const wrong: Promise<number> = simple.add({ userId: '1' }).result;

const composed = createTaskEngine({
  worker: async (input: { url: string }, ctx) => {
    const context: TaskContext = ctx;
    void context;
    return { length: input.url.length };
  },
  plugins: [
    fifo(), concurrency(4), timeout(1000),
    retry({ maxAttempts: 3, shouldRetry: error => error instanceof Error }),
    latestBy((task: Task<{ url: string }>) => task.input.url, { cancelRunning: true }),
  ],
});
const engine: TaskEngine<{ url: string }, { length: number }> = composed;
const handles: TaskHandle<{ length: number }>[] = composed.addMany([{ url: '/a' }, { url: '/b' }]);
composed.subscribe(state => {
  const url: string | undefined = state.pending[0]?.input.url;
  // @ts-expect-error snapshots preserve input type
  const wrong: number | undefined = state.pending[0]?.input.url;
  void url; void wrong;
});
// @ts-expect-error dispatch keeps result type
composed.dispatch(composed.state.tr.complete('a', 'run', 42));
const ranked = createTaskEngine({
  worker: (input: { rank: number }) => input.rank,
  plugins: [priority({ getPriority: task => task.input.rank })],
});
const rank: Promise<number> = ranked.add({ rank: 1 }).result;

const configured: TaskEngineOptions<string, number> = {
  worker: input => input.length,
  plugins: [fifo(), concurrency(1)],
};
const configuredEngine: TaskEngine<string, number> = createTaskEngine(configured);
void name; void wrong; void engine; void handles; void rank; void configuredEngine;

const savedPolicies = [fifo(), concurrency(2), retry({ maxAttempts: 2 })];
const reused = createTaskEngine({ worker: (input: number) => input.toString(), plugins: savedPolicies });
const text: Promise<string> = reused.add(1).result;
const foreignPolicy = latestBy((task: Task<{ other: boolean }>) => task.input.other);
createTaskEngine({
  worker: (input: { url: string }) => input.url,
  // @ts-expect-error input-specific policy cannot read unrelated worker data
  plugins: [foreignPolicy],
});
const wrongResultPlugin = definePlugin<string, boolean, undefined>({ key: new PluginKey<undefined>('wrong-result') });
createTaskEngine({
  worker: (input: string) => input.length,
  // @ts-expect-error result-specific plugin must not widen the worker result type
  plugins: [wrongResultPlugin],
});
const externalPolicy = definePluginFactory(<Input, Result>() => definePlugin<Input, Result, undefined>({
  key: new PluginKey<undefined>('external'),
  filterTransaction: () => true,
}));
const external = createTaskEngine({ worker: (input: boolean) => Number(input), plugins: [externalPolicy] });
const num: Promise<number> = external.add(true).result;
void text; void num;
