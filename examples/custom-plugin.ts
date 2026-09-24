import {
  createTaskEngine,
  definePlugin,
  PluginKey,
} from '@yuqgnort/taskloom';

interface CounterState {
  readonly enqueued: number;
}

const counterKey = new PluginKey<CounterState>('counter');
const counter = definePlugin<string, number, CounterState>({
  key: counterKey,
  state: {
    init: () => ({ enqueued: 0 }),
    apply: (transaction, value) => ({
      enqueued: value.enqueued
        + transaction.steps.filter(step => step.type === 'enqueue').length,
    }),
  },
});

const engine = createTaskEngine({
  worker: (input: string) => input.length,
  plugins: [counter],
});

await engine.add('transactional plugins').result;
console.log(counterKey.getState(engine.state));
engine.destroy();
