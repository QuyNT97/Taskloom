import {
  definePlugin, instantiatePlugins, PluginKey, TaskRuntime, EngineDestroyedError,
  type TaskState, type TaskTransaction, type TaskHandle,
  type TaskRuntimeOptions, type TaskPluginSource, type ApplyTransactionResult, type AddTaskOptions,
} from '@task-engine/kernel';
import { fifo } from '@task-engine/plugins';

export interface TaskEngine<I, R> {
  readonly state: TaskState<I, R>;
  readonly paused: boolean;
  readonly destroyed: boolean;
  readonly error: unknown;
  add(input: I, options?: AddTaskOptions): TaskHandle<R>;
  addMany(inputs: readonly I[]): TaskHandle<R>[];
  cancel(taskId: string, reason?: unknown): void;
  clear(): void;
  pause(): void;
  resume(): void;
  dispatch(tr: TaskTransaction<I, R>): ApplyTransactionResult<I, R>;
  subscribe(listener: (state: TaskState<I, R>) => void): () => void;
  destroy(): void;
}

export interface TaskEngineOptions<I, R> extends Omit<TaskRuntimeOptions<I, R>, 'plugins'> {
  readonly plugins?: NoInfer<readonly TaskPluginSource<I, R>[]>;
}

/** Infer input/result from the worker; policy factories specialize afterwards. */
export function createTaskEngine<I, R>(options: TaskEngineOptions<I, R>): TaskEngine<I, R> {
  const controlKey = new PluginKey<boolean, boolean>('engine-pause');
  const control = definePlugin<I, R, boolean, boolean>({
    key: controlKey,
    state: { init: () => false, apply: (tr, value) => tr.getMeta(controlKey) ?? value },
    scheduling: { canStart: (_, state) => !controlKey.getState(state) },
    filterTransaction: (tr, state) => !(tr.isExecute && controlKey.getState(state)),
  });
  const plugins = instantiatePlugins<I, R>(options.plugins ?? []);
  if (!plugins.some(plugin => plugin.scheduling?.pickNext)) plugins.push(fifo<I>().create<I, R>());
  plugins.push(control);
  const runtime = new TaskRuntime<I, R>({
    ...options,
    plugins,
  });
  const setPaused = (value: boolean): void => {
    if (runtime.destroyed) throw new EngineDestroyedError();
    if (controlKey.getState(runtime.state) !== value) runtime.dispatch(runtime.state.tr.setMeta(controlKey, value));
  };
  return Object.freeze({
    get state() { return runtime.state; },
    get paused() { return controlKey.getState(runtime.state) ?? false; },
    get destroyed() { return runtime.destroyed; },
    get error() { return runtime.error; },
    add: (input: I, options?: AddTaskOptions) => runtime.add(input, options),
    addMany: (inputs: readonly I[]) => runtime.addMany(inputs),
    cancel: (taskId: string, reason?: unknown) => runtime.cancel(taskId, reason),
    clear: () => runtime.clear(),
    pause: () => setPaused(true),
    resume: () => setPaused(false),
    dispatch: (tr: TaskTransaction<I, R>) => runtime.dispatch(tr),
    subscribe: (listener: (state: TaskState<I, R>) => void) => runtime.subscribe(listener),
    destroy: () => runtime.destroy(),
  });
}
