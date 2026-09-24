import { snapshot } from './immutable.js';
import { SnapshotMap, type Task, type RunningTask } from './task.js';
import { TaskTransaction } from './transaction.js';
import { instantiatePlugins, type PluginKey, type TaskPlugin, type TaskPluginSource } from './plugin.js';
import { runTransactionPipeline, type ApplyTransactionOptions, type ApplyTransactionResult } from './pipeline.js';

export class TaskState<I = unknown, R = unknown> {
  private constructor(
    readonly version: number,
    readonly pending: readonly Task<I>[],
    readonly running: ReadonlyMap<string, RunningTask<I>>,
    readonly plugins: readonly TaskPlugin<I, R>[],
    private readonly pluginValues: ReadonlyMap<object, unknown>,
  ) { Object.freeze(this); }

  static create<I = unknown, R = unknown>(config: { readonly plugins?: readonly TaskPluginSource<I, R>[] } = {}): TaskState<I, R> {
    const plugins = Object.freeze(instantiatePlugins(config.plugins ?? []).map(plugin => Object.freeze({ ...plugin })));
    const keys = new Set<object>();
    for (const plugin of plugins) {
      if (keys.has(plugin.key)) throw new Error(`Duplicate PluginKey: ${plugin.name}`);
      keys.add(plugin.key);
    }
    const base = new TaskState<I, R>(0, Object.freeze([]), new SnapshotMap(), plugins, new SnapshotMap());
    const values = plugins.map(plugin => [plugin.key, snapshot(plugin.initState({ state: base }))] as const);
    return new TaskState(0, base.pending, base.running, plugins, new SnapshotMap(values));
  }
  get tr(): TaskTransaction<I, R> { return new TaskTransaction(this); }
  getPluginState<S, M>(key: PluginKey<S, M>): S | undefined { return this.pluginValues.get(key) as S | undefined; }

  /** Runs filters and append hooks to completion, returning only the final state. */
  apply(tr: TaskTransaction<I, R>, options: ApplyTransactionOptions = {}): TaskState<I, R> {
    return this.applyTransaction(tr, options).state;
  }

  /** Pure dispatch pipeline: publish the returned state only after this succeeds. */
  applyTransaction(tr: TaskTransaction<I, R>, options: ApplyTransactionOptions = {}): ApplyTransactionResult<I, R> {
    return runTransactionPipeline(this, tr, (state, transaction) => state.#applyInner(transaction), options);
  }

  #applyInner(tr: TaskTransaction<I, R>): TaskState<I, R> {
    if (tr.before !== this) throw new Error('Transaction belongs to a different state snapshot');
    const pending = new Map(this.pending.map(task => [task.id, task]));
    const running = new Map(this.running);
    for (const step of tr.steps) {
      if (step.type === 'enqueue') {
        if (!step.task.id || !Number.isFinite(step.task.createdAt)) throw new Error('Task requires an ID and finite createdAt');
        if (pending.has(step.task.id) || running.has(step.task.id)) throw new Error(`Duplicate task ID: ${step.task.id}`);
        pending.set(step.task.id, step.task);
        continue;
      }
      const id = step.taskId;
      if (step.type === 'start') {
        const task = pending.get(id);
        if (!task) throw new Error(`Cannot start non-pending task: ${id}`);
        if (!step.executionId || !Number.isFinite(step.startedAt)) throw new Error('Start requires executionId and finite startedAt');
        pending.delete(id);
        running.set(id, Object.freeze({ task, executionId: step.executionId, startedAt: step.startedAt }));
      } else if (step.type === 'complete' || step.type === 'fail') {
        if (running.get(id)?.executionId !== step.executionId) throw new Error(`No matching execution for task: ${id}`);
        running.delete(id);
      } else {
        if (!pending.has(id) && !running.has(id)) throw new Error(`Unknown task: ${id}`);
        pending.delete(id);
        running.delete(id);
      }
    }
    const next = new TaskState(this.version + 1, Object.freeze([...pending.values()]), new SnapshotMap(running), this.plugins, this.pluginValues);
    // Every reducer sees the same core snapshot and previous plugin values.
    const values = this.plugins.map(plugin => [plugin.key, snapshot(plugin.applyState(tr, this.pluginValues.get(plugin.key), this, next))] as const);
    return new TaskState(next.version, next.pending, next.running, next.plugins, new SnapshotMap(values));
  }
}
