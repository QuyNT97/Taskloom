import type { Task, TaskStatus } from '../task.js';
import type { TaskHandle } from './types.js';

/** Retained only while active; a caller's handle owns terminal status/result. */
export class TaskRecord<I, R> {
  status: TaskStatus = 'pending';
  attempt = 0;
  outcome: unknown;
  readonly handle: TaskHandle<R>;
  readonly #resolve: (result: R | PromiseLike<R>) => void;
  readonly #reject: (reason: unknown) => void;

  constructor(public task: Task<I>, cancel: (record: TaskRecord<I, R>, reason?: unknown) => void) {
    let resolve!: (result: R | PromiseLike<R>) => void;
    let reject!: (reason: unknown) => void;
    const result = new Promise<R>((yes, no) => { resolve = yes; reject = no; });
    // Raw dispatch can create tasks without consumers; the original promise still
    // rejects for awaiters, while background cancellation is never unhandled.
    void result.catch(() => undefined);
    this.#resolve = resolve;
    this.#reject = reject;
    const record = this;
    this.handle = Object.freeze({
      id: task.id,
      result,
      get status() { return record.status; },
      cancel: (reason?: unknown) => cancel(record, reason),
    });
  }

  settle(): void {
    if (this.status === 'succeeded') this.#resolve(this.outcome as R);
    else this.#reject(this.outcome);
  }
}
