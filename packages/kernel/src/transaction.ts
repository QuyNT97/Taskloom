import type { Task } from './task.js';
import type { TaskState } from './state.js';
import type { PluginKey } from './plugin.js';

export type TransactionStep<I, R> =
  | { readonly type: 'enqueue'; readonly task: Task<I> }
  | { readonly type: 'remove' | 'cancel'; readonly taskId: string; readonly reason?: unknown }
  | { readonly type: 'start'; readonly taskId: string; readonly executionId: string; readonly startedAt: number }
  | { readonly type: 'complete'; readonly taskId: string; readonly executionId: string; readonly result: R }
  | { readonly type: 'fail'; readonly taskId: string; readonly executionId: string; readonly error: unknown };

export class TaskTransaction<I = unknown, R = unknown> {
  readonly steps: readonly TransactionStep<I, R>[];
  readonly #meta: ReadonlyMap<string | object, unknown>;
  constructor(readonly before: TaskState<I, R>, steps: readonly TransactionStep<I, R>[] = [], meta: ReadonlyMap<string | object, unknown> = new Map()) {
    this.steps = Object.freeze(steps.map(step => Object.freeze(step.type === 'enqueue'
      ? { ...step, task: Object.freeze({ ...step.task, ...(step.task.meta ? { meta: Object.freeze({ ...step.task.meta }) } : {}) }) }
      : { ...step })));
    this.#meta = new Map(meta);
    Object.freeze(this);
  }
  private add(step: TransactionStep<I, R>): TaskTransaction<I, R> {
    return new TaskTransaction(this.before, [...this.steps, step], this.#meta);
  }
  enqueue(task: Task<I>): TaskTransaction<I, R> { return this.add({ type: 'enqueue', task }); }
  remove(taskId: string): TaskTransaction<I, R> { return this.add({ type: 'remove', taskId }); }
  cancel(taskId: string, reason?: unknown): TaskTransaction<I, R> { return this.add({ type: 'cancel', taskId, reason }); }
  start(taskId: string, executionId: string, startedAt: number): TaskTransaction<I, R> { return this.add({ type: 'start', taskId, executionId, startedAt }); }
  complete(taskId: string, executionId: string, result: R): TaskTransaction<I, R> { return this.add({ type: 'complete', taskId, executionId, result }); }
  fail(taskId: string, executionId: string, error: unknown): TaskTransaction<I, R> { return this.add({ type: 'fail', taskId, executionId, error }); }
  setMeta(key: string, value: unknown): TaskTransaction<I, R>;
  setMeta<S, M>(key: PluginKey<S, M>, value: M): TaskTransaction<I, R>;
  setMeta(key: string | object, value: unknown): TaskTransaction<I, R> {
    return new TaskTransaction(this.before, this.steps, new Map([...this.#meta, [key, value]]));
  }
  setPluginMeta<S, M>(key: PluginKey<S, M>, value: M): TaskTransaction<I, R> { return this.setMeta(key, value); }
  getMeta(key: string): unknown;
  getMeta<S, M>(key: PluginKey<S, M>): M | undefined;
  getMeta(key: string | object): unknown { return this.#meta.get(key); }
  get isEnqueue(): boolean { return this.steps.some(step => step.type === 'enqueue'); }
  get isExecute(): boolean { return this.steps.some(step => step.type === 'start'); }
}
