import { describe, test, expect } from 'vitest';
import { DataArray } from '../../src/DataArray';

describe('BUG 8: method=nearest throws for values beyond the coordinate range', () => {
  test('nearest without tolerance clamps to the edge coordinate', async () => {
    const da = new DataArray([1, 2, 3], { dims: ['x'], coords: { x: [0, 10, 20] } });

    // xarray: sel(26, method='nearest') -> coord 20
    const res = await da.sel({ x: 26 }, { method: 'nearest' });
    expect(res.data).toBe(3);
  });
});
