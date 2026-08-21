/**
 * Regression: concatenating two lazy zarr-backed DataArrays must return usable
 * data for a range that STRADDLES the seam between them.
 *
 * Zarr loaders return flat `{ data, shape }` payloads, not nested JS arrays.
 * `concatenateArrays` branched on `Array.isArray`, so a flat payload fell into
 * the scalar path and came back as `[first, second]` — a two-element array of
 * objects. The declared shape stayed correct, so the mismatch only surfaced
 * later in `compute()`:
 *
 *   Coordinate 'time' length (3) does not match dimension size (2)
 *
 * Only straddling reads are affected: a range landing entirely inside one
 * operand is forwarded to that operand untouched and never reaches the merge.
 */

import { describe, expect, test } from 'vitest';
import { DataArray } from '../../src/DataArray';
import { Dataset } from '../../src/Dataset';
import type { FlatData } from '../../src/types';

/**
 * A lazy DataArray backed by flat row-major storage, mirroring what the zarr
 * backend hands to concat (Float64Array + shape, never nested arrays).
 */
function flatLazyArray(times: string[], values: number[]): DataArray {
  const shape = [times.length];
  return new DataArray(null as never, {
    lazy: true,
    virtualShape: shape,
    lazyLoader: async (ranges: Record<string, { start: number; stop: number } | number>) => {
      const range = ranges.time;
      let start = 0;
      let stop = times.length;
      if (typeof range === 'number') {
        start = range;
        stop = range + 1;
      } else if (range) {
        start = range.start;
        stop = range.stop;
      }
      const slice = values.slice(start, stop);
      return { data: Float64Array.from(slice), shape: [slice.length] } as FlatData as never;
    },
    dims: ['time'],
    coords: { time: times }
  } as never);
}

function flatLazyDataset(times: string[], values: number[]): Dataset {
  return new Dataset({ t2m: flatLazyArray(times, values) }, { coords: { time: times } });
}

describe('concat across a flat-data seam', () => {
  const firstTimes = ['2026-05-31T21:00:00Z', '2026-05-31T22:00:00Z'];
  const secondTimes = ['2026-05-31T23:00:00Z', '2026-06-01T00:00:00Z'];

  test('materializes a range spanning both operands', async () => {
    const combined = flatLazyDataset(firstTimes, [100, 101])
      .concat(flatLazyDataset(secondTimes, [200, 201]), { dim: 'time' });

    expect(combined.sizes.time).toBe(4);

    // Straddles the seam: two values from the first operand, one from the second.
    const straddle = await combined.isel({ time: [0, 1, 2] });
    const computed = await straddle.getVariable('t2m').compute();

    expect(computed.shape).toEqual([3]);
    expect(Array.from(computed.values as ArrayLike<number>)).toEqual([100, 101, 200]);
  });

  test('a full materialization keeps every value in order', async () => {
    const combined = flatLazyDataset(firstTimes, [100, 101])
      .concat(flatLazyDataset(secondTimes, [200, 201]), { dim: 'time' });

    const computed = await combined.getVariable('t2m').compute();

    expect(computed.shape).toEqual([4]);
    expect(Array.from(computed.values as ArrayLike<number>)).toEqual([100, 101, 200, 201]);
  });

  test('reads confined to one operand still work', async () => {
    const combined = flatLazyDataset(firstTimes, [100, 101])
      .concat(flatLazyDataset(secondTimes, [200, 201]), { dim: 'time' });

    const firstOnly = await combined.isel({ time: [0, 1] });
    const secondOnly = await combined.isel({ time: [2, 3] });

    expect(Array.from((await firstOnly.getVariable('t2m').compute()).values as ArrayLike<number>))
      .toEqual([100, 101]);
    expect(Array.from((await secondOnly.getVariable('t2m').compute()).values as ArrayLike<number>))
      .toEqual([200, 201]);
  });
});
