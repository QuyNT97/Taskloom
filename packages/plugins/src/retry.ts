import {
  definePlugin, definePluginFactory, PluginKey, TransactionRejectedError,
  type Task, type TaskPluginFactory, type TaskTransaction,
} from '@yuqgnort/taskloom-kernel';
import { MAX_TIMER_DELAY } from './timeout.js';

export interface RetryEntry {
  readonly attempt: number;
  readonly generation: number;
  readonly delay: number | null;
}
export type RetryState = ReadonlyMap<string, RetryEntry>;
export type RetryMeta = readonly (
  | { readonly type: 'wait'; readonly id: string; readonly entry: RetryEntry }
  | { readonly type: 'ready'; readonly id: string; readonly generation: number; readonly attempt: number }
)[];
export const retryKey = new PluginKey<RetryState, RetryMeta>('retry');

export interface RetryContext<I> {
  readonly task: Task<I>;
  /** The attempt that just failed; maxAttempts includes the initial execution. */
  readonly attempt: number;
}
export interface RetryOptions<I> {
  readonly maxAttempts: number;
  readonly backoff?: {
    readonly type: 'fixed' | 'exponential';
    readonly base: number;
    readonly max?: number;
  };
  readonly shouldRetry?: (error: unknown, context: RetryContext<I>) => boolean;
}

function failedTasks<I, R>(transactions: readonly TaskTransaction<I, R>[]) {
  const failures = new Map<string, { task: Task<I>; error: unknown; entry: RetryEntry }>();
  for (const tr of transactions) {
    const active = new Map([
      ...tr.before.pending.map(task => [task.id, task] as const),
      ...[...tr.before.running.values()].map(({ task }) => [task.id, task] as const),
    ]);
    const attempts = new Map(retryKey.getState(tr.before));
    for (const step of tr.steps) {
      if (step.type === 'enqueue') {
        failures.delete(step.task.id);
        active.set(step.task.id, step.task);
        attempts.set(step.task.id, attempts.get(step.task.id) ?? { attempt: 0, generation: tr.before.version + 1, delay: null });
      } else if (step.type === 'start') {
        const entry = attempts.get(step.taskId);
        attempts.set(step.taskId, { attempt: (entry?.attempt ?? 0) + 1, generation: entry?.generation ?? tr.before.version + 1, delay: null });
      }
      else if (step.type === 'fail') {
        const task = active.get(step.taskId);
        const entry = attempts.get(step.taskId);
        if (task && entry) failures.set(step.taskId, { task, error: step.error, entry });
        active.delete(step.taskId);
        attempts.delete(step.taskId);
      } else {
        failures.delete(step.taskId);
        active.delete(step.taskId);
        attempts.delete(step.taskId);
      }
    }
  }
  return failures;
}

export function retry<I = never>(options: RetryOptions<I>): TaskPluginFactory<I> {
  const { maxAttempts, shouldRetry } = options;
  const backoff = options.backoff ? Object.freeze({ ...options.backoff }) : undefined;
  if (!Number.isSafeInteger(maxAttempts) || maxAttempts < 1) throw new RangeError('maxAttempts must be a positive safe integer');
  if (backoff && (!['fixed', 'exponential'].includes(backoff.type)
    || !Number.isFinite(backoff.base) || backoff.base < 0 || backoff.base > MAX_TIMER_DELAY
    || (backoff.max !== undefined && (!Number.isFinite(backoff.max) || backoff.max < 0 || backoff.max > MAX_TIMER_DELAY)))) {
    throw new RangeError('Invalid retry backoff type, base or maximum delay');
  }
  const getDelay = (attempt: number): number => !backoff || backoff.base === 0 ? 0
    : Math.min(backoff.max ?? MAX_TIMER_DELAY, backoff.base * (backoff.type === 'exponential' ? 2 ** (attempt - 1) : 1));

  return definePluginFactory<I>(<Input, Result>() => definePlugin<Input, Result, RetryState, RetryMeta>({
    key: retryKey,
    state: {
      init: () => new Map(),
      apply(tr, value, oldState, newState) {
        const entries = new Map(value);
        for (const step of tr.steps) {
          if (step.type === 'enqueue') {
            const previous = entries.get(step.task.id);
            entries.set(step.task.id, {
              attempt: previous?.attempt ?? 0,
              generation: previous?.generation ?? newState.version,
              delay: null,
            });
          } else if (step.type === 'start') {
            const entry = entries.get(step.taskId);
            entries.set(step.taskId, { attempt: (entry?.attempt ?? 0) + 1, generation: entry?.generation ?? newState.version, delay: null });
          } else entries.delete(step.taskId);
        }
        for (const message of tr.getMeta(retryKey) ?? []) {
          const entry = entries.get(message.id);
          if (entry && message.type === 'wait') entries.set(message.id, message.entry);
          else if (entry && message.type === 'ready' && entry.generation === message.generation && entry.attempt === message.attempt) {
            entries.set(message.id, { ...entry, delay: null });
          }
        }
        return entries;
      },
    },
    scheduling: { canStart: (task, state) => retryKey.getState(state)?.get(task.id)?.delay == null },
    appendTransaction(transactions, oldState, newState) {
      let tr = newState.tr;
      const messages: RetryMeta[number][] = [];
      for (const [id, failed] of failedTasks(transactions)) {
        const entry = failed.entry;
        if (newState.pending.some(task => task.id === id) || newState.running.has(id)) continue;
        const allowed = entry.attempt < maxAttempts && (shouldRetry ? shouldRetry(failed.error, {
          task: failed.task as unknown as Task<I>, attempt: entry.attempt,
        }) : true);
        if (typeof allowed !== 'boolean') throw new TypeError('shouldRetry must return a synchronous boolean');
        if (allowed) {
          tr = tr.enqueue(failed.task);
          const delay = getDelay(entry.attempt);
          messages.push({ type: 'wait', id, entry: { ...entry, delay: delay > 0 ? delay : null } });
        }
      }
      return messages.length ? tr.setMeta(retryKey, messages) : null;
    },
    runtime: {
      setup(ctx) {
        const timers = new Map<string, { entry: RetryEntry; cancel: () => void }>();
        const same = (a: RetryEntry, b: RetryEntry | undefined): boolean =>
          a.attempt === b?.attempt && a.generation === b.generation && a.delay === b.delay;
        return {
          onTransaction() {
            const entries = retryKey.getState(ctx.state)!;
            for (const [id, timer] of timers) {
              if (!same(timer.entry, entries.get(id))) { timer.cancel(); timers.delete(id); }
            }
            for (const [id, entry] of entries) {
              if (entry.delay === null || timers.has(id)) continue;
              const cancel = ctx.clock.setTimeout(() => {
                timers.delete(id);
                if (!same(entry, retryKey.getState(ctx.state)?.get(id))) return;
                const tr = ctx.state.tr.setMeta(retryKey, [{ type: 'ready', id, generation: entry.generation, attempt: entry.attempt }]);
                if (!ctx.dispatch(tr).transactions.length) throw new TransactionRejectedError('retry readiness');
              }, entry.delay);
              timers.set(id, { entry, cancel });
            }
          },
          destroy() {
            for (const timer of timers.values()) timer.cancel();
            timers.clear();
          },
        };
      },
    },
  }));
}
