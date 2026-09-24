// These test-only plugins use the package export, with no core-internal imports.
import { definePlugin, PluginKey } from '@task-engine/core';

export function dedupeBy(getKey) {
  return definePlugin({
    key: new PluginKey('external-dedupe'),
    filterTransaction(tr, state) {
      const active = new Map([
        ...state.pending.map(task => [task.id, getKey(task)]),
        ...[...state.running.values()].map(({ task }) => [task.id, getKey(task)]),
      ]);
      for (const step of tr.steps) {
        if (step.type === 'enqueue') {
          const key = getKey(step.task);
          if (new Set(active.values()).has(key)) return false;
          active.set(step.task.id, key);
        } else if (step.type !== 'start') {
          active.delete(step.taskId);
        }
      }
      return true;
    },
  });
}

export function taskCounter() {
  const key = new PluginKey('external-counter');
  const plugin = definePlugin({
    key,
    state: {
      init: () => ({ enqueued: 0 }),
      apply: (tr, value) => ({
        enqueued: value.enqueued + tr.steps.filter(step => step.type === 'enqueue').length,
      }),
    },
  });
  return { key, plugin };
}
