import { describe, test, expect } from 'vitest';
import { DataArray } from '../../src/DataArray';

describe('BUG 9: mean() counts null (masked) values in the denominator', () => {
  test('where(cond).mean() should average only valid values', () => {
    const da = new DataArray([1, 2, 3, 4], { dims: ['x'], coords: { x: [0, 1, 2, 3] } });
    const cond = new DataArray([true, true, false, false], {
      dims: ['x'],
      coords: { x: [0, 1, 2, 3] }
    });

    const masked = da.where(cond); // [1, 2, null, null]
    expect(masked.data).toEqual([1, 2, null, null]);

    // xarray: (1 + 2) / 2 = 1.5 (masked values excluded)
    expect(masked.mean()).toBe(1.5);
  });

  test('where(cond).mean(dim) should average only valid values', () => {
    const da = new DataArray([1, 2, 3, 4], { dims: ['x'], coords: { x: [0, 1, 2, 3] } });
    const cond = new DataArray([true, true, false, false], {
      dims: ['x'],
      coords: { x: [0, 1, 2, 3] }
    });

    const masked = da.where(cond); // [1, 2, null, null]
    expect(masked.data).toEqual([1, 2, null, null]);

    // xarray: (1 + 2) / 2 = 1.5 (masked values excluded)
    expect(masked.mean('x')).toBe(1.5);
  });
});
