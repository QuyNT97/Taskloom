import { SnapshotMap } from './task.js';

/** Plugin state is data: snapshot it so retained references cannot mutate history. */
export function snapshot(value: unknown, ancestors = new Set<object>()): unknown {
  if (value === null || typeof value !== 'object') {
    if (typeof value === 'function') throw new Error('Plugin state must contain data, not functions');
    return value;
  }
  if (ancestors.has(value)) throw new Error('Plugin state must not contain cycles');
  const next = new Set(ancestors).add(value);
  const copy = (entry: unknown): unknown => snapshot(entry, next);
  if (value instanceof Map || value instanceof SnapshotMap) {
    return new SnapshotMap([...value].map(([key, entry]) => {
      if (key !== null && (typeof key === 'object' || typeof key === 'function')) throw new Error('Plugin state maps require primitive keys');
      return [key, copy(entry)] as const;
    }));
  }
  if (Array.isArray(value)) return Object.freeze(value.map(copy));
  const proto: unknown = Object.getPrototypeOf(value);
  if (proto !== Object.prototype && proto !== null) throw new Error('Plugin state supports plain objects, arrays and readonly maps');
  const result: Record<PropertyKey, unknown> = Object.create(null) as Record<PropertyKey, unknown>;
  for (const key of Reflect.ownKeys(value)) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (!descriptor || !('value' in descriptor)) throw new Error('Plugin state must not contain accessors');
    Object.defineProperty(result, key, { value: copy(descriptor.value), enumerable: descriptor.enumerable ?? false });
  }
  return Object.freeze(result);
}
