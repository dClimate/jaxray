import { describe, test, expect } from 'vitest';
import { DataArray } from '../../src/DataArray';

describe('BUG 4: sel with slice + method=nearest breaks coordinate slicing', () => {
  test('slice with inexact endpoints and nearest should return matched coords', async () => {
    const da = new DataArray([10, 20, 30, 40], { dims: ['x'], coords: { x: [0, 10, 20, 30] } });

    // data path resolves nearest indices 1..2, coordinate path uses exact indexOf and fails
    const res = await da.sel({ x: { start: 9, stop: 21 } }, { method: 'nearest' });
    expect(res.data).toEqual([20, 30]);
    expect(res.coords.x).toEqual([10, 20]);
  });
});
