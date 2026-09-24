import { definePlugin, definePluginFactory, PluginKey, type Task, type TaskPluginFactory } from '@task-engine/kernel';

export function fifo<I = unknown>(): TaskPluginFactory<I> {
  return definePluginFactory<I>(<Input extends I, Result>() => definePlugin<Input, Result, undefined>({
    key: new PluginKey<undefined>('fifo'),
    scheduling: {
      pickNext: (_, candidates) => candidates.reduce<Task<Input> | undefined>(
        (best, task) => !best || task.createdAt < best.createdAt ? task : best, undefined,
      ),
    },
  }));
}

export function concurrency<I = unknown>(limit: number): TaskPluginFactory<I> {
  if (!Number.isSafeInteger(limit) || limit < 1) throw new RangeError('concurrency limit must be a positive safe integer');
  return definePluginFactory<I>(<Input extends I, Result>() => definePlugin<Input, Result, undefined>({
    key: new PluginKey<undefined>('concurrency'),
    scheduling: { canStart: (_, state, context) => context.activeWorkers < limit },
  }));
}

export interface PriorityOptions<I> {
  readonly getPriority?: (task: Task<I>) => number;
}

export function priority<I = unknown>(options: PriorityOptions<I> = {}): TaskPluginFactory<I> {
  const getPriority = options.getPriority;
  return definePluginFactory<I>(<Input extends I, Result>() => definePlugin<Input, Result, undefined>({
    key: new PluginKey<undefined>('priority'),
    scheduling: {
      pickNext(_, candidates) {
        let best: Task<Input> | undefined;
        let bestPriority = -Infinity;
        for (const task of candidates) {
          const value = getPriority ? getPriority(task) : task.meta?.priority ?? 0;
          if (typeof value !== 'number' || !Number.isFinite(value)) throw new TypeError('Task priority must be a finite number');
          if (!best || value > bestPriority || (value === bestPriority && task.createdAt < best.createdAt)) {
            best = task;
            bestPriority = value;
          }
        }
        return best;
      },
    },
  }));
}
