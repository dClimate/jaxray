import { describe, test, expect } from 'vitest';
import { DataArray } from '../../src/DataArray';

/** 1D lazy loader over base data; number range drops the dim (same convention as existing tests). */
function makeLazy1D(baseData: number[], coords: number[]) {
  const loader = async (ranges: { [dim: string]: { start: number; stop: number } | number }) => {
    const r = ranges.x;
    if (typeof r === 'number') {
      return baseData[r];
    }
    if (r && typeof r === 'object') {
      return baseData.slice(r.start, r.stop);
    }
    return [...baseData];
  };

  return new DataArray(null, {
    lazy: true,
    virtualShape: [baseData.length],
    lazyLoader: loader,
    dims: ['x'],
    coords: { x: coords }
  });
}

describe('BUG 1: lazy chained selection double-maps scalar indices', () => {
  test('slice then scalar sel on lazy array fetches the wrong element', async () => {
    const lazy = makeLazy1D([100, 101, 102, 103, 104, 105], [0, 10, 20, 30, 40, 50]);

    // First selection: slice coords 20..50 (original indices 2..5)
    const sliced = await lazy.sel({ x: { start: 20, stop: 50 } });
    expect(sliced.coords.x).toEqual([20, 30, 40, 50]);

    // Second selection: scalar coord 30 -> original index 3 -> value 103
    const scalar = await sliced.sel({ x: 30 });
    expect(scalar.data).toBe(103);
  });

  test('discrete array sel then scalar sel fetches the wrong element', async () => {
    const lazy = makeLazy1D([100, 101, 102, 103, 104, 105], [0, 10, 20, 30, 40, 50]);

    const picked = await lazy.sel({ x: [0, 20, 50] }); // original indices [0, 2, 5]
    const scalar = await picked.sel({ x: 20 }); // original index 2 -> 102
    expect(scalar.data).toBe(102);
  });
});
