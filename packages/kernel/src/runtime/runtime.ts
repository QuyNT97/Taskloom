import { TaskState } from '../state.js';
import type { TaskPluginSource } from '../plugin.js';
import type { Task, RunningTask } from '../task.js';
import type { TaskTransaction } from '../transaction.js';
import type { ApplyTransactionOptions, ApplyTransactionResult } from '../pipeline.js';
import { TaskRecord } from './handle.js';
import { EngineDestroyedError, TaskCancelledError, TransactionRejectedError } from './errors.js';
import type {
  AddTaskOptions, RuntimeClock, RuntimeSetupContext, TaskContext,
  TaskHandle, TaskSchedulingSpec, TaskWorker, TaskRuntimeHooks,
} from './types.js';

export interface TaskRuntimeOptions<I, R> extends ApplyTransactionOptions {
  readonly worker: TaskWorker<I, R>;
  /** Worker defines input/result types; generic policies must not widen them. */
  readonly plugins: NoInfer<readonly TaskPluginSource<I, R>[]>;
  readonly clock?: RuntimeClock;
  /** Receives automatic scheduling/hook/cleanup failures, not ordinary task failures. */
  readonly onError?: (error: unknown) => void;
}

interface Execution<I, R> {
  readonly running: RunningTask<I>;
  readonly record: TaskRecord<I, R>;
  readonly controller: AbortController;
  readonly cleanup: (() => void)[];
  settled: boolean;
}

const systemClock: RuntimeClock = Object.freeze({
  now: () => Date.now(),
  setTimeout(callback: () => void, delay: number) {
    const timer = setTimeout(callback, delay);
    return () => clearTimeout(timer);
  },
});

/** Advanced execution primitive; policy defaults are composed by the facade. */
export class TaskRuntime<I = unknown, R = unknown> {
  #state: TaskState<I, R>;
  readonly #worker: TaskWorker<I, R>;
  readonly #clock: RuntimeClock;
  readonly #options: ApplyTransactionOptions;
  readonly #onError: ((error: unknown) => void) | undefined;
  readonly #pickNext: NonNullable<TaskSchedulingSpec<I, R>['pickNext']>;
  readonly #records = new Map<string, TaskRecord<I, R>>();
  readonly #executions = new Map<string, Execution<I, R>>();
  // Cancellation invalidates an execution immediately, but capacity remains
  // occupied until the worker's promise really settles.
  readonly #workers = new Set<Execution<I, R>>();
  readonly #cleanup: (() => void)[] = [];
  readonly #hooks: TaskRuntimeHooks<I, R>[] = [];
  readonly #effects: (() => void)[] = [];
  readonly #listeners = new Set<{ readonly callback: (state: TaskState<I, R>) => void }>();
  #taskSequence = 0;
  #executionSequence = 0;
  #scheduled = false;
  #busy = false;
  #destroyed = false;
  #error: unknown;

  constructor(options: TaskRuntimeOptions<I, R>) {
    this.#state = TaskState.create({ plugins: options.plugins });
    const selectors = this.#state.plugins.flatMap(plugin => plugin.scheduling?.pickNext ? [plugin.scheduling.pickNext] : []);
    if (selectors.length !== 1) throw new Error('TaskRuntime requires exactly one scheduling.pickNext provider');
    this.#pickNext = selectors[0]!;
    this.#worker = options.worker;
    const clock = options.clock ?? systemClock;
    this.#clock = Object.freeze({
      now: () => clock.now(),
      setTimeout: (callback: () => void, delay: number) => clock.setTimeout(() => {
        if (this.#destroyed) return;
        try { callback(); }
        catch (error) { this.#shutdown(error, 'failed'); this.#report(error); }
      }, delay),
    });
    this.#onError = options.onError;
    this.#options = options.maxAppendedTransactions === undefined ? {} : { maxAppendedTransactions: options.maxAppendedTransactions };
    try {
      for (const plugin of this.#state.plugins) {
        const spec = plugin.runtime;
        if (!spec) continue;
        const instance = spec.setup?.(this.#context());
        if (instance !== null && typeof instance === 'object') {
          if ('then' in instance) throw new TypeError('Runtime setup must be synchronous');
          this.#hooks.push(Object.freeze({ ...spec, ...instance }));
          this.#registerCleanup(instance.destroy, this.#cleanup);
        } else {
          this.#hooks.push(spec);
          this.#registerCleanup(instance, this.#cleanup);
        }
      }
    } catch (error) {
      this.#shutdown(error, 'failed');
      throw error;
    }
  }

  get state(): TaskState<I, R> { return this.#state; }
  get destroyed(): boolean { return this.#destroyed; }
  get error(): unknown { return this.#error; }
  get activeWorkers(): number { return this.#workers.size; }

  add(input: I, options: AddTaskOptions = {}): TaskHandle<R> {
    return this.#enqueue([{ input, options }])[0]!;
  }

  /** A single transaction: every input is accepted or every handle is rejected. */
  addMany(inputs: readonly I[]): TaskHandle<R>[] {
    return this.#enqueue(inputs.map(input => ({ input, options: {} })));
  }

  #enqueue(entries: readonly { input: I; options: AddTaskOptions }[]): TaskHandle<R>[] {
    this.#assertWritable();
    if (!entries.length) return [];
    const records: TaskRecord<I, R>[] = [];
    try {
      let tr = this.#state.tr;
      for (const { input, options } of entries) {
        let id = options.id;
        if (id === undefined) {
          do { id = `task-${++this.#taskSequence}`; } while (this.#records.has(id));
        }
        if (this.#records.has(id)) throw new Error(`Duplicate task ID: ${id}`);
        const task: Task<I> = { id, input, createdAt: this.#clock.now(), ...(options.meta ? { meta: options.meta } : {}) };
        const record = this.#createRecord(task);
        records.push(record);
        this.#records.set(id, record);
        tr = tr.enqueue(task);
      }
      const result = this.dispatch(tr);
      if (!result.transactions.length) {
        for (const record of records) {
          record.status = 'failed';
          record.outcome = new TransactionRejectedError('enqueue');
          this.#finish(record);
        }
      }
    } catch (error) {
      for (const record of records) {
        record.status = 'failed';
        record.outcome = error;
        this.#finish(record);
      }
      throw error;
    }
    return records.map(record => record.handle);
  }

  /** Future commits only; callbacks are deferred and individually isolated. */
  subscribe(callback: (state: TaskState<I, R>) => void): () => void {
    this.#assertWritable();
    const subscription = { callback };
    this.#listeners.add(subscription);
    return () => { this.#listeners.delete(subscription); };
  }

  /** Cancel the active task set in one ordinary, filterable transaction. */
  clear(): void {
    this.#assertWritable();
    if (!this.#records.size) return;
    let tr = this.#state.tr;
    for (const id of this.#records.keys()) tr = tr.cancel(id);
    this.dispatch(tr);
  }

  cancel(taskId: string, reason?: unknown): void {
    this.#assertWritable();
    if (!this.#records.has(taskId)) return;
    this.dispatch(this.#state.tr.cancel(taskId, reason));
  }

  dispatch(tr: TaskTransaction<I, R>): ApplyTransactionResult<I, R> {
    this.#assertWritable();
    const oldState = this.#state;
    this.#busy = true;
    let result: ApplyTransactionResult<I, R>;
    try { result = oldState.applyTransaction(tr, this.#options); }
    finally { this.#busy = false; }
    if (!result.transactions.length) return result;

    // Nothing observable is published until the entire pure pipeline succeeds.
    this.#state = result.state;
    this.#updateRecords(result);
    const listeners = [...this.#listeners];
    this.#effects.push(() => {
      const context = this.#context({ oldState, result });
      for (const hooks of this.#hooks) {
        if (this.#destroyed) break;
        const returned: unknown = hooks.onTransaction?.(context);
        if (returned !== undefined) throw new TypeError('onTransaction must return synchronously with no value');
      }
      for (const subscription of listeners) {
        if (!this.#listeners.has(subscription)) continue;
        try {
          // A JS/async listener may return a promise despite the void callback
          // signature. Observe rejection without delaying subsequent listeners.
          void Promise.resolve(subscription.callback(result.state)).catch(error => {
            if (!this.#destroyed) this.#report(error);
          });
        }
        catch (error) { this.#report(error); }
      }
    });
    // Detach all stale executions before aborting: abort listeners can dispatch.
    const stopped: Execution<I, R>[] = [];
    for (const [id, execution] of this.#executions) {
      if (this.#state.running.get(id) !== execution.running || this.#records.get(id) !== execution.record) {
        this.#executions.delete(id);
        stopped.push(execution);
      }
    }
    for (const execution of stopped) this.#release(execution);
    this.wake();
    return result;
  }

  /** Coalesced microtask wakeup; safe for stale timers after destruction. */
  wake(): void {
    if (this.#scheduled || this.#destroyed) return;
    this.#scheduled = true;
    queueMicrotask(() => {
      this.#scheduled = false;
      if (this.#destroyed) return;
      try { this.#pump(); }
      catch (error) {
        this.#shutdown(error, 'failed');
        this.#report(error);
      }
    });
  }

  destroy(): void {
    if (this.#destroyed) return;
    this.#assertWritable();
    this.#shutdown(new EngineDestroyedError(), 'cancelled');
  }

  #assertWritable(): void {
    if (this.#destroyed) throw new EngineDestroyedError();
    if (this.#busy) throw new Error('Runtime mutation is not allowed inside pure state or scheduling hooks');
  }

  #context<E extends object = Record<never, never>>(extra?: E): RuntimeSetupContext<I, R> & E {
    const runtime = this;
    return Object.freeze({
      get state() { return runtime.state; },
      clock: this.#clock,
      dispatch: (tr: TaskTransaction<I, R>) => this.dispatch(tr),
      cancel: (id: string, reason?: unknown) => this.cancel(id, reason),
      wake: () => this.wake(),
      ...extra,
    }) as RuntimeSetupContext<I, R> & E;
  }

  #createRecord(task: Task<I>): TaskRecord<I, R> {
    return new TaskRecord(task, (record, reason) => {
      // A settled handle cannot cancel a different logical task reusing its ID.
      if (!this.#destroyed && this.#records.get(task.id) === record) this.cancel(task.id, reason);
    });
  }

  #updateRecords(result: ApplyTransactionResult<I, R>): void {
    for (const tr of result.transactions) {
      for (const step of tr.steps) {
        if (step.type === 'enqueue') {
          const record = this.#records.get(step.task.id) ?? this.#createRecord(step.task);
          record.task = step.task;
          record.status = 'pending';
          this.#records.set(step.task.id, record);
          continue;
        }
        const record = this.#records.get(step.taskId);
        if (!record) throw new Error(`Missing runtime task record: ${step.taskId}`);
        if (step.type === 'start') {
          record.status = 'running';
          record.attempt++;
        } else if (step.type === 'complete') {
          record.status = 'succeeded';
          record.outcome = step.result;
        } else if (step.type === 'fail') {
          record.status = 'failed';
          record.outcome = step.error;
        } else {
          record.status = 'cancelled';
          record.outcome = new TaskCancelledError(step.taskId, step.reason);
          this.#finish(record);
        }
      }
    }
    // A fail/complete followed by enqueue retains the logical handle. Cancel or
    // remove closes it immediately; reuse after cancellation creates a new one.
    for (const record of this.#records.values()) {
      if (record.status === 'succeeded' || record.status === 'failed') this.#finish(record);
    }
  }

  #finish(record: TaskRecord<I, R>): void {
    if (this.#records.get(record.task.id) === record) this.#records.delete(record.task.id);
    record.settle();
  }

  #pump(): void {
    // Hooks may dispatch more work; process that in a later turn, not recursively.
    const effects = this.#effects.splice(0);
    for (const effect of effects) {
      if (this.#destroyed) return;
      effect();
    }
    if (this.#effects.length) { this.wake(); return; }
    for (const running of this.#state.running.values()) {
      if (this.#destroyed) return;
      if (!this.#executions.has(running.task.id)) this.#launch(running);
    }
    if (this.#destroyed || this.#effects.length) { this.wake(); return; }

    const attempted = new Set<string>();
    const budget = this.#state.pending.length;
    for (let index = 0; index < budget; index++) {
      this.#busy = true;
      let selected: Task<I> | undefined;
      try {
        const context = Object.freeze({ activeWorkers: this.activeWorkers });
        const candidates = Object.freeze(this.#state.pending.filter(task => !attempted.has(task.id)
          && this.#state.plugins.every(plugin => {
            const canStart = plugin.scheduling?.canStart;
            const admitted = canStart ? canStart(task, this.#state, context) : true;
            if (typeof admitted !== 'boolean') throw new TypeError('canStart must return a synchronous boolean');
            return admitted;
          })));
        selected = this.#pickNext(this.#state, candidates, context);
        if (selected !== undefined && !candidates.includes(selected)) {
          throw new Error('Scheduler must select a task from the supplied candidates');
        }
      } finally { this.#busy = false; }
      if (!selected) return;
      attempted.add(selected.id);
      const result = this.dispatch(this.#state.tr.start(selected.id, `execution-${++this.#executionSequence}`, this.#clock.now()));
      // One accepted selection per turn: notifications run before its worker,
      // and a rejected head does not prevent another eligible task being chosen.
      if (result.transactions.length) return;
    }
  }

  #launch(running: RunningTask<I>): void {
    if (this.#state.running.get(running.task.id) !== running) return;
    const record = this.#records.get(running.task.id)!;
    const execution: Execution<I, R> = { running, record, controller: new AbortController(), cleanup: [], settled: false };
    this.#executions.set(running.task.id, execution);
    const context: TaskContext = Object.freeze({
      taskId: running.task.id, executionId: running.executionId,
      startedAt: running.startedAt, attempt: record.attempt, signal: execution.controller.signal,
    });
    const runtimeContext = this.#context({ ...context, task: running.task });
    for (const hooks of this.#hooks) {
      if (!this.#isCurrent(execution)) return;
      const cleanup = hooks.onTaskStart?.(runtimeContext);
      this.#registerCleanup(cleanup, execution.cleanup);
      if (!this.#isCurrent(execution)) { this.#release(execution); return; }
    }
    this.#workers.add(execution);
    let work: R | PromiseLike<R>;
    try { work = this.#worker(running.task.input, context); }
    catch (error) { this.#settled(execution, { ok: false, error }); return; }
    void Promise.resolve(work).then(
      result => this.#settled(execution, { ok: true, result }),
      error => this.#settled(execution, { ok: false, error }),
    );
  }

  #isCurrent(execution: Execution<I, R>): boolean {
    return !this.#destroyed && this.#executions.get(execution.running.task.id) === execution
      && this.#state.running.get(execution.running.task.id) === execution.running;
  }

  #settled(execution: Execution<I, R>, outcome: { ok: true; result: R } | { ok: false; error: unknown }): void {
    execution.settled = true;
    this.#workers.delete(execution);
    if (!this.#isCurrent(execution)) { this.wake(); return; }
    try {
      const { task, executionId } = execution.running;
      const tr = outcome.ok ? this.#state.tr.complete(task.id, executionId, outcome.result)
        : this.#state.tr.fail(task.id, executionId, outcome.error);
      if (!this.dispatch(tr).transactions.length) throw new TransactionRejectedError('worker settlement');
    } catch (error) {
      // A consumed worker outcome cannot be silently left in running state.
      this.#shutdown(error, 'failed');
      this.#report(error);
    }
    this.wake();
  }

  #registerCleanup(value: unknown, target: (() => void)[]): void {
    if (value === undefined) return;
    if (typeof value !== 'function') throw new TypeError('Runtime setup/start hooks must return synchronously with a cleanup function or undefined');
    target.push(value as () => void);
  }

  #release(execution: Execution<I, R>): void {
    if (!execution.settled && !execution.controller.signal.aborted) {
      execution.controller.abort(execution.record.outcome ?? new TaskCancelledError(execution.running.task.id));
    }
    this.#runCleanup(execution.cleanup);
  }

  #runCleanup(cleanup: (() => void)[]): void {
    for (const callback of cleanup.splice(0).reverse()) {
      try { callback(); } catch (error) { this.#report(error); }
    }
  }

  #shutdown(reason: unknown, status: 'failed' | 'cancelled'): void {
    if (this.#destroyed) return;
    this.#destroyed = true;
    this.#listeners.clear();
    this.#effects.length = 0;
    for (const record of this.#records.values()) {
      record.status = status;
      record.outcome = reason;
      record.settle();
    }
    this.#records.clear();
    const executions = [...this.#executions.values()];
    this.#executions.clear();
    for (const execution of executions) this.#release(execution);
    this.#runCleanup(this.#cleanup);
  }

  #report(error: unknown): void {
    this.#error = error;
    try { this.#onError?.(error); } catch { /* Error reporters cannot prevent teardown. */ }
  }
}
