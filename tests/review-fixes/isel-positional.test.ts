import { describe, test, expect } from 'vitest';
import { DataArray } from '../../src/DataArray';

describe('BUG 5: isel goes through coordinate labels (position -> label -> position)', () => {
  test('isel with duplicate coordinate values returns the wrong element', async () => {
    const da = new DataArray([10, 11, 12], { dims: ['x'], coords: { x: [5, 5, 7] } });

    const res = await da.isel({ x: 1 });
    expect(res.data).toBe(11); // positional index 1
  });

  test('isel with out-of-bounds index should throw, not silently ignore', async () => {
    const da = new DataArray([10, 11, 12], { dims: ['x'], coords: { x: [0, 1, 2] } });

    await expect(da.isel({ x: 10 })).rejects.toThrow();
  });
});
