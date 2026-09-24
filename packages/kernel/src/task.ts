export interface Task<TInput = unknown> {
  readonly id: string;
  readonly input: TInput;
  readonly createdAt: number;
  readonly meta?: Readonly<Record<string, unknown>>;
}
export type TaskStatus = 'pending' | 'running' | 'succeeded' | 'failed' | 'cancelled';
export interface RunningTask<TInput = unknown> {
  readonly task: Task<TInput>;
  readonly executionId: string;
  readonly startedAt: number;
}

/** A real read-only facade: consumers cannot cast back to a mutable Map. */
export class SnapshotMap<K, V> implements ReadonlyMap<K, V> {
  readonly #values: Map<K, V>;
  constructor(entries: Iterable<readonly [K, V]> = []) {
    this.#values = new Map(entries);
    Object.freeze(this);
  }
  get size(): number { return this.#values.size; }
  get(key: K): V | undefined { return this.#values.get(key); }
  has(key: K): boolean { return this.#values.has(key); }
  entries(): MapIterator<[K, V]> { return this.#values.entries(); }
  keys(): MapIterator<K> { return this.#values.keys(); }
  values(): MapIterator<V> { return this.#values.values(); }
  [Symbol.iterator](): MapIterator<[K, V]> { return this.entries(); }
  forEach(callback: (value: V, key: K, map: ReadonlyMap<K, V>) => void, thisArg?: unknown): void {
    this.#values.forEach((value, key) => callback.call(thisArg, value, key, this));
  }
}
