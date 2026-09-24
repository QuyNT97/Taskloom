import type { TaskState } from './state.js';
import { TaskTransaction } from './transaction.js';

export interface ApplyTransactionOptions {
  /** Maximum accepted appends, excluding the root transaction. Default: 100. */
  readonly maxAppendedTransactions?: number;
}

export interface ApplyTransactionResult<I = unknown, R = unknown> {
  readonly state: TaskState<I, R>;
  /** Empty when the root was filtered out; otherwise root followed by appends. */
  readonly transactions: readonly TaskTransaction<I, R>[];
}

export class AppendTransactionLimitError extends Error {
  constructor(readonly limit: number, readonly pluginName: string) {
    super(`Append transaction limit (${limit}) exceeded by plugin: ${pluginName}`);
    this.name = 'AppendTransactionLimitError';
  }
}

function accepts<I, R>(state: TaskState<I, R>, tr: TaskTransaction<I, R>): boolean {
  // Check identity before filtering: a veto must not hide a stale transaction.
  if (!(tr instanceof TaskTransaction)) throw new TypeError('Expected a TaskTransaction');
  if (tr.before !== state) throw new Error('Transaction belongs to a different state snapshot');
  for (const plugin of state.plugins) {
    if (!plugin.filterTransaction) continue;
    const accepted = plugin.filterTransaction(tr, state);
    if (typeof accepted !== 'boolean') {
      throw new TypeError(`filterTransaction must return a synchronous boolean: ${plugin.name}`);
    }
    if (!accepted) return false;
  }
  return true;
}

/** Internal orchestration; only TaskState supplies the primitive reducer. */
export function runTransactionPipeline<I, R>(
  initial: TaskState<I, R>,
  root: TaskTransaction<I, R>,
  reduce: (state: TaskState<I, R>, tr: TaskTransaction<I, R>) => TaskState<I, R>,
  options: ApplyTransactionOptions,
): ApplyTransactionResult<I, R> {
  const limit = options.maxAppendedTransactions ?? 100;
  if (!Number.isSafeInteger(limit) || limit < 0) {
    throw new RangeError('maxAppendedTransactions must be a non-negative safe integer');
  }
  if (!accepts(initial, root)) {
    return Object.freeze({ state: initial, transactions: Object.freeze([]) });
  }

  let state = reduce(initial, root);
  const transactions = [root];
  const appenders = initial.plugins.flatMap(plugin => plugin.appendTransaction
    ? [{ plugin, append: plugin.appendTransaction, seen: 0, oldState: initial }]
    : []);

  // Cursors advance even after null or vetoed proposals. Only new accepted work
  // can wake an appender again, so rejected proposals cannot spin this loop.
  for (;;) {
    const countBeforePass = transactions.length;
    for (const cursor of appenders) {
      if (cursor.seen === transactions.length) continue;
      const unseen = Object.freeze(transactions.slice(cursor.seen));
      const appended = cursor.append(unseen, cursor.oldState, state);
      if (appended !== null && appended !== undefined) {
        if (!(appended instanceof TaskTransaction)) {
          throw new TypeError(`appendTransaction must return a synchronous TaskTransaction, null or undefined: ${cursor.plugin.name}`);
        }
        if (accepts(state, appended)) {
          if (transactions.length - 1 >= limit) {
            throw new AppendTransactionLimitError(limit, cursor.plugin.name);
          }
          state = reduce(state, appended);
          transactions.push(appended);
        }
      }
      // The origin implicitly knows its own accepted append. Its next oldState
      // is after that append; it is never notified solely about its own output.
      cursor.seen = transactions.length;
      cursor.oldState = state;
    }
    if (transactions.length === countBeforePass) break;
  }
  return Object.freeze({ state, transactions: Object.freeze(transactions) });
}
