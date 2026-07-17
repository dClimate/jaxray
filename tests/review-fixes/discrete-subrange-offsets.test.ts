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

describe('BUG 2: lazy sub-range fetch after discrete array selection', () => {
  test('slice of a discrete selection extracts wrong offsets', async () => {
    const lazy = makeLazy1D([100, 101, 102, 103, 104, 105], [0, 10, 20, 30, 40, 50]);

    const picked = await lazy.sel({ x: [0, 20, 50] }); // original indices [0, 2, 5]
    const sub = await picked.sel({ x: { start: 20, stop: 50 } }); // child coords [20, 50]
    expect(sub.coords.x).toEqual([20, 50]);

    const computed = await sub.compute();
    expect(computed.data).toEqual([102, 105]);
  });
});
