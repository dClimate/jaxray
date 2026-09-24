import { describe, test, expect } from 'vitest';
import { DataArray } from '../../src/DataArray';

// Spreading an array into Math.min/Math.max passes every element as a call
// argument, and V8 throws "Maximum call stack size exceeded" somewhere past
// ~120k of them. A selection that size is ordinary: ~14 years of an hourly
// axis. dclimate-client-js's timeRange() hands isel() one index per matching
// timestamp, so ERA5 hourly 1940..2026 (~750k indices) failed outright.
const LENGTH = 200_000;
const SOURCE = Array.from({ length: LENGTH }, (_, index) => index);

function makeLazyArray(): DataArray {
  const lazyLoader = async (ranges: Record<string, { start: number; stop: number } | number>) => {
    const range = ranges.time ?? { start: 0, stop: LENGTH };
    if (typeof range === 'number') return SOURCE[range];
    return SOURCE.slice(range.start, range.stop);
  };
  return new DataArray(null, {
    lazy: true,
    virtualShape: [LENGTH],
    lazyLoader,
    dims: ['time'],
    coords: { time: SOURCE }
  });
}

describe('selections larger than the call-argument limit', () => {
  test('lazy isel with an index per element', async () => {
    const indices = Array.from({ length: LENGTH }, (_, index) => index);

    const computed = await (await makeLazyArray().isel({ time: indices })).compute();

    expect(computed.data).toHaveLength(LENGTH);
    expect(computed.data[0]).toBe(0);
    expect(computed.data[LENGTH - 1]).toBe(LENGTH - 1);
  });

  test('lazy isel with a large sparse selection (every other element)', async () => {
    const indices = Array.from({ length: LENGTH / 2 + 1 }, (_, index) => Math.min(index * 2, LENGTH - 1));

    const computed = await (await makeLazyArray().isel({ time: indices })).compute();

    expect(computed.data).toHaveLength(indices.length);
    expect(computed.data[1]).toBe(2);
  });

  test('lazy sel with a large coordinate list', async () => {
    const computed = await (await makeLazyArray().sel({ time: SOURCE })).compute();

    expect(computed.data).toHaveLength(LENGTH);
  });
});
