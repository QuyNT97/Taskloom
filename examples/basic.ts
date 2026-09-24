import {
  createTaskEngine,
  concurrency,
  fifo,
  retry,
  timeout,
} from '@yuqgnort/taskloom';

interface RequestInput {
  readonly url: string;
}

const engine = createTaskEngine({
  worker: async (input: RequestInput, context) => {
    const response = await fetch(input.url, { signal: context.signal });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response.json() as Promise<unknown>;
  },
  plugins: [
    fifo(),
    concurrency(4),
    timeout(5_000),
    retry({
      maxAttempts: 3,
      backoff: { type: 'exponential', base: 250, max: 2_000 },
    }),
  ],
});

const unsubscribe = engine.subscribe(state => {
  console.log(`pending=${state.pending.length} running=${state.running.size}`);
});

const handle = engine.add({ url: 'https://example.com/data.json' });

try {
  console.log(await handle.result);
} finally {
  unsubscribe();
  engine.destroy();
}
