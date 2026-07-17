import { describe, test, expect } from 'vitest';
import { DataArray } from '../../src/DataArray';

describe('BUG 6: exact sel silently matches values far from any coordinate', () => {
  test('sel without method should throw for a value not in the index', async () => {
    const da = new DataArray([1, 2], { dims: ['x'], coords: { x: [0, 1000] } });

    // 0.5 is not a coordinate; relative tolerance (1e-3 of step=1000) wrongly matches index 0
    await expect(da.sel({ x: 0.5 })).rejects.toThrow();
  });
});
