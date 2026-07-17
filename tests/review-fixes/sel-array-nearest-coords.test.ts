import { describe, test, expect } from 'vitest';
import { DataArray } from '../../src/DataArray';

describe('BUG 3: sel with array + method=nearest keeps requested values as coords', () => {
  test('result coords should be the actual matched coordinate values', async () => {
    const da = new DataArray([10, 20, 30], { dims: ['x'], coords: { x: [1, 2, 3] } });

    const res = await da.sel({ x: [1.1, 2.2] }, { method: 'nearest' });
    expect(res.data).toEqual([10, 20]);
    // xarray returns the actual index values that were matched, not the requested ones
    expect(res.coords.x).toEqual([1, 2]);
  });
});
