import { describe, test, expect } from 'vitest';
import { DataArray } from '../../src/DataArray';

describe('BUG 6: exact sel silently matches values far from any coordinate', () => {
  test('sel without method should throw for a value not in the index', async () => {
    const da = new DataArray([1, 2], { dims: ['x'], coords: { x: [0, 1000] } });

    // 0.5 is not a coordinate; relative tolerance (1e-3 of step=1000) wrongly matches index 0
    await expect(da.sel({ x: 0.5 })).rejects.toThrow();
  });

  test('sel rejects an offset that is a meaningful fraction of a fine-grid step', async () => {
    const da = new DataArray([1, 2], { dims: ['x'], coords: { x: [0, 1e-8] } });

    await expect(da.sel({ x: 5e-10 })).rejects.toThrow();
  });

  test('sel rejects a tiny non-coordinate offset on a coarse grid', async () => {
    const da = new DataArray([1, 2], { dims: ['x'], coords: { x: [0, 1000] } });

    await expect(da.sel({ x: 1e-9 })).rejects.toThrow();
  });

  test('sel still resolves exact coordinates and genuine floating-point noise', async () => {
    const fine = new DataArray([1, 2], { dims: ['x'], coords: { x: [0, 1e-8] } });
    const coarse = new DataArray([1, 2], { dims: ['x'], coords: { x: [0, 1000] } });
    const floatCoords = Array.from({ length: 10 }, (_, i) => 0.1 * i);
    const floats = new DataArray(floatCoords, { dims: ['x'], coords: { x: floatCoords } });

    expect((await fine.sel({ x: 1e-8 })).data).toBe(2);
    expect((await coarse.sel({ x: 0 })).data).toBe(1);
    expect((await floats.sel({ x: 0.1 * 3 })).data).toBe(0.1 * 3);
    expect((await floats.sel({ x: 0.3 })).data).toBe(0.1 * 3);
  });
});
