import { definePlugin, definePluginFactory, PluginKey, type Task, type TaskPluginFactory } from '@task-engine/kernel';

export interface LatestOptions {
  /** Default false: replace pending tasks only. */
  readonly cancelRunning?: boolean;
}

export function latestBy<I = unknown, K = unknown>(
  getKey: (task: Task<I>) => K,
  options: LatestOptions = {},
): TaskPluginFactory<I> {
  const cancelRunning = options.cancelRunning ?? false;
  return definePluginFactory<I>(<Input extends I, Result>() => definePlugin<Input, Result, undefined>({
    key: new PluginKey<undefined>('latest'),
    appendTransaction(transactions, oldState, newState) {
      const active = new Map([
        ...newState.pending.map(task => [task.id, task] as const),
        ...[...newState.running.values()].map(({ task }) => [task.id, task] as const),
      ]);
      const latest = new Map<K, Task<I>>();
      for (const tr of transactions) {
        for (const step of tr.steps) {
          if (step.type !== 'enqueue' || active.get(step.task.id) !== step.task) continue;
          const key = getKey(step.task);
          if (key !== undefined && key !== null) latest.set(key, step.task);
        }
      }
      let tr = newState.tr;
      const replaceable = cancelRunning ? [...active.values()] : newState.pending;
      for (const task of replaceable) {
        const replacement = latest.get(getKey(task));
        if (replacement && replacement.id !== task.id) {
          tr = tr.cancel(task.id, Object.freeze({ type: 'superseded', replacementId: replacement.id }));
        }
      }
      return tr.steps.length ? tr : null;
    },
  }));
}
