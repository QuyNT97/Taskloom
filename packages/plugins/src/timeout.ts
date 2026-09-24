import { definePlugin, definePluginFactory, PluginKey, TransactionRejectedError, type TaskPluginFactory } from '@task-engine/kernel';

export const MAX_TIMER_DELAY = 2_147_483_647;

export class TaskTimeoutError extends Error {
  constructor(readonly taskId: string, readonly timeoutMs: number) {
    super(`Task timed out after ${timeoutMs}ms: ${taskId}`);
    this.name = 'TaskTimeoutError';
  }
}

export function timeout<I = unknown>(milliseconds: number): TaskPluginFactory<I> {
  if (!Number.isFinite(milliseconds) || milliseconds < 0 || milliseconds > MAX_TIMER_DELAY) {
    throw new RangeError(`timeout must be between 0 and ${MAX_TIMER_DELAY} milliseconds`);
  }
  return definePluginFactory<I>(<Input extends I, Result>() => definePlugin<Input, Result, undefined>({
    key: new PluginKey<undefined>('timeout'),
    runtime: {
      onTaskStart(ctx) {
        return ctx.clock.setTimeout(() => {
          if (ctx.state.running.get(ctx.taskId)?.executionId !== ctx.executionId) return;
          const error = new TaskTimeoutError(ctx.taskId, milliseconds);
          if (!ctx.dispatch(ctx.state.tr.fail(ctx.taskId, ctx.executionId, error)).transactions.length) {
            throw new TransactionRejectedError('timeout');
          }
        }, milliseconds);
      },
    },
  }));
}
