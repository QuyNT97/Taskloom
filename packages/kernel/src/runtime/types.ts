import type { Task, TaskStatus } from '../task.js';
import type { TaskState } from '../state.js';
import type { TaskTransaction } from '../transaction.js';
import type { ApplyTransactionResult } from '../pipeline.js';

export interface TaskContext {
  readonly taskId: string;
  readonly executionId: string;
  readonly signal: AbortSignal;
  readonly attempt: number;
  readonly startedAt: number;
}

export type TaskWorker<I, R> = (input: I, context: TaskContext) => R | PromiseLike<R>;

export interface TaskHandle<R> {
  readonly id: string;
  readonly result: Promise<R>;
  readonly status: TaskStatus;
  cancel(reason?: unknown): void;
}

export interface RuntimeClock {
  now(): number;
  /** Returns an idempotent function that cancels the timer. */
  setTimeout(callback: () => void, delay: number): () => void;
}

export interface SchedulingContext {
  /** Includes aborted workers until their promises actually settle. */
  readonly activeWorkers: number;
}

export interface TaskSchedulingSpec<I, R> {
  /** Exactly one plugin must supply selection. Return one of the candidates. */
  readonly pickNext?: (
    state: TaskState<I, R>,
    candidates: readonly Task<I>[],
    context: SchedulingContext,
  ) => Task<I> | undefined;
  /** All admission predicates must agree; no ordering/capacity policy in core. */
  readonly canStart?: (task: Task<I>, state: TaskState<I, R>, context: SchedulingContext) => boolean;
}

export interface RuntimeSetupContext<I, R> {
  readonly state: TaskState<I, R>;
  readonly clock: RuntimeClock;
  dispatch(tr: TaskTransaction<I, R>): ApplyTransactionResult<I, R>;
  cancel(taskId: string, reason?: unknown): void;
  /** Re-evaluate selection after an external timer/resource change. */
  wake(): void;
}

export interface RuntimeTaskContext<I, R> extends RuntimeSetupContext<I, R>, TaskContext {
  readonly task: Task<I>;
}

export interface RuntimeTransactionContext<I, R> extends RuntimeSetupContext<I, R> {
  readonly oldState: TaskState<I, R>;
  /** Committed result for this notification; state may since have advanced. */
  readonly result: ApplyTransactionResult<I, R>;
}

export interface TaskRuntimeHooks<I, R> {
  readonly onTransaction?: (context: RuntimeTransactionContext<I, R>) => undefined;
  readonly onTaskStart?: (context: RuntimeTaskContext<I, R>) => void | (() => void);
}

/** Per-runtime resources; safe even when a plugin definition is shared. */
export interface TaskRuntimePluginInstance<I, R> extends TaskRuntimeHooks<I, R> {
  readonly destroy?: () => void;
}

export interface TaskRuntimePluginSpec<I, R> extends TaskRuntimeHooks<I, R> {
  readonly setup?: (context: RuntimeSetupContext<I, R>) => void | (() => void) | TaskRuntimePluginInstance<I, R>;
}

export interface AddTaskOptions {
  readonly id?: string;
  readonly meta?: Readonly<Record<string, unknown>>;
}
