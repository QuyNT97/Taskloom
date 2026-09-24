import type { TaskState } from './state.js';
import type { TaskTransaction } from './transaction.js';
import type { TaskRuntimePluginSpec, TaskSchedulingSpec } from './runtime/types.js';

export class PluginKey<TState = unknown, TMeta = unknown> {
  // Phantom fields preserve both type parameters without global key counters.
  declare private readonly stateType: TState;
  declare private readonly metaType: TMeta;
  constructor(readonly name = 'plugin') { Object.freeze(this); }
  getState<I, R>(state: TaskState<I, R>): TState | undefined {
    return state.getPluginState(this);
  }
}
export interface PluginInitContext<I, R> {
  readonly state: TaskState<I, R>;
}
export interface TaskTransactionHooks<I, R> {
  /** Synchronous, whole-transaction veto. All filters also inspect appends. */
  readonly filterTransaction?: (tr: TaskTransaction<I, R>, state: TaskState<I, R>) => boolean;
  /** Receives only unseen accepted transactions; build from newState.tr. */
  readonly appendTransaction?: (
    transactions: readonly TaskTransaction<I, R>[],
    oldState: TaskState<I, R>,
    newState: TaskState<I, R>,
  ) => TaskTransaction<I, R> | null | undefined;
}
export interface TaskPluginExtensions<I, R> extends TaskTransactionHooks<I, R> {
  readonly scheduling?: TaskSchedulingSpec<I, R>;
  readonly runtime?: TaskRuntimePluginSpec<I, R>;
}
export interface TaskPluginSpec<I, R, S, M = unknown> extends TaskPluginExtensions<I, R> {
  readonly key: PluginKey<S, M>;
  readonly state?: {
    readonly init: (context: PluginInitContext<I, R>) => S;
    readonly apply: (tr: TaskTransaction<I, R>, value: S, oldState: TaskState<I, R>, newState: TaskState<I, R>) => S;
  };
}
/** Existential state is erased only at the collection boundary. */
export interface TaskPlugin<I = unknown, R = unknown> extends TaskPluginExtensions<I, R> {
  readonly key: object;
  readonly name: string;
  readonly initState: (context: PluginInitContext<I, R>) => unknown;
  readonly applyState: (tr: TaskTransaction<I, R>, value: unknown, oldState: TaskState<I, R>, newState: TaskState<I, R>) => unknown;
}

declare const factoryInput: unique symbol;

/** A policy can be specialized for any worker result without erasing its types. */
export interface TaskPluginFactory<I = never> {
  /** Type-only input constraint; broad policies accept narrower worker inputs. */
  readonly [factoryInput]?: I;
  readonly create: <Input, Result>() => TaskPlugin<Input, Result>;
}

export type TaskPluginSource<I, R> = TaskPlugin<I, R> | TaskPluginFactory<I>;

export function definePluginFactory<I = never>(create: TaskPluginFactory<I>['create']): TaskPluginFactory<I> {
  return Object.freeze({ create });
}

export function instantiatePlugins<I, R>(plugins: readonly TaskPluginSource<I, R>[]): TaskPlugin<I, R>[] {
  return plugins.map(plugin => 'create' in plugin ? plugin.create<I, R>() : plugin);
}
export function definePlugin<I = unknown, R = unknown, S = unknown, M = unknown>(spec: TaskPluginSpec<I, R, S, M>): TaskPlugin<I, R> {
  const init = spec.state?.init;
  const apply = spec.state?.apply;
  const { filterTransaction, appendTransaction } = spec;
  return Object.freeze({
    key: spec.key,
    name: spec.key.name,
    ...(filterTransaction ? { filterTransaction } : {}),
    ...(appendTransaction ? { appendTransaction } : {}),
    ...(spec.scheduling ? { scheduling: Object.freeze({ ...spec.scheduling }) } : {}),
    ...(spec.runtime ? { runtime: Object.freeze({ ...spec.runtime }) } : {}),
    initState: (ctx: PluginInitContext<I, R>) => init?.(ctx),
    applyState: (tr: TaskTransaction<I, R>, value: unknown, oldState: TaskState<I, R>, newState: TaskState<I, R>) =>
      apply ? apply(tr, value as S, oldState, newState) : value,
  });
}
