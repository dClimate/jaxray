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

function makeLazy2D(baseData: number[][], xCoords: number[], yCoords: number[]) {
  const loader = async (ranges: { [dim: string]: { start: number; stop: number } | number }) => {
    const xRange = ranges.x ?? { start: 0, stop: baseData.length };
    const yRange = ranges.y ?? { start: 0, stop: baseData[0].length };
    const rows = typeof xRange === 'number'
      ? [baseData[xRange]]
      : baseData.slice(xRange.start, xRange.stop);
    const result = rows.map(row => typeof yRange === 'number'
      ? [row[yRange]]
      : row.slice(yRange.start, yRange.stop));

    if (typeof xRange === 'number' && typeof yRange === 'number') return result[0][0];
    if (typeof xRange === 'number') return result[0];
    if (typeof yRange === 'number') return result.map(row => row[0]);
    return result;
  };

  return new DataArray(null, {
    lazy: true,
    virtualShape: [baseData.length, baseData[0].length],
    lazyLoader: loader,
    dims: ['x', 'y'],
    coords: { x: xCoords, y: yCoords }
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

  test('scalar sibling selection preserves the axis of a prior discrete selection', async () => {
    const baseData = Array.from({ length: 3 }, (_, x) =>
      Array.from({ length: 5 }, (_, y) => x * 10 + y)
    );
    const lazy = makeLazy2D(baseData, [0, 1, 2], [100, 110, 120, 130, 140]);

    const picked = await lazy.sel({ y: [100, 120, 140] });
    const row = await picked.sel({ x: 1 });
    const computed = await row.compute();

    expect(computed.data).toEqual([10, 12, 14]);
    expect(computed.dims).toEqual(['y']);
    expect(computed.shape).toEqual([3]);
  });
});
