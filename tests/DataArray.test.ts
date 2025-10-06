/**
 * Tests for DataArray
 */

import { describe, test, expect } from 'vitest';
import { DataArray } from '../src/DataArray';

describe('DataArray', () => {
  test('should create a simple 1D DataArray', () => {
    const data = [1, 2, 3, 4, 5];
    const da = new DataArray(data, { dims: ['x'] });

    expect(da.data).toEqual(data);
    expect(da.dims).toEqual(['x']);
    expect(da.shape).toEqual([5]);
    expect(da.ndim).toBe(1);
    expect(da.size).toBe(5);
  });

  test('should create a 2D DataArray', () => {
    const data = [
      [1, 2, 3],
      [4, 5, 6]
    ];
    const da = new DataArray(data, {
      dims: ['y', 'x']
    });

    expect(da.data).toEqual(data);
    expect(da.dims).toEqual(['y', 'x']);
    expect(da.shape).toEqual([2, 3]);
    expect(da.ndim).toBe(2);
    expect(da.size).toBe(6);
  });

  test('should create DataArray with custom coordinates', () => {
    const data = [1, 2, 3];
    const da = new DataArray(data, {
      dims: ['time'],
      coords: {
        time: ['2021-01-01', '2021-01-02', '2021-01-03']
      }
    });

    expect(da.coords['time']).toEqual(['2021-01-01', '2021-01-02', '2021-01-03']);
  });

  test('should auto-generate dimension names if not provided', () => {
    const data = [[1, 2], [3, 4]];
    const da = new DataArray(data);

    expect(da.dims).toEqual(['dim_0', 'dim_1']);
  });

  test('should select data by label using sel()', () => {
    const data = [1, 2, 3, 4, 5];
    const da = new DataArray(data, {
      dims: ['x'],
      coords: {
        x: [10, 20, 30, 40, 50]
      }
    });

    const selected = da.sel({ x: 30 });
    expect(selected).toBe(3);
  });

  test('should select multiple values using sel()', async () => {
    const data = [1, 2, 3, 4, 5];
    const da = new DataArray(data, {
      dims: ['x'],
      coords: {
        x: [10, 20, 30, 40, 50]
      }
    });

    const selected = await da.sel({ x: [10, 30, 50] });
    expect(selected.data).toEqual([1, 3, 5]);
  });

  test('should slice data using sel()', async () => {
    const data = [1, 2, 3, 4, 5];
    const da = new DataArray(data, {
      dims: ['x'],
      coords: {
        x: [10, 20, 30, 40, 50]
      }
    });

    const selected = await da.sel({ x: { start: 20, stop: 40 } });
    expect(selected.data).toEqual([2, 3, 4]);
  });

  test('should select by integer position using isel()', async () => {
    const data = [1, 2, 3, 4, 5];
    const da = new DataArray(data, { dims: ['x'] });

    const selected = await da.isel({ x: 2 });
    expect(selected.data).toBe(3);
  });

  test('should compute sum along dimension', () => {
    const data = [
      [1, 2, 3],
      [4, 5, 6]
    ];
    const da = new DataArray(data, {
      dims: ['y', 'x']
    });

    const sumX = da.sum('x');
    expect(sumX).toBeInstanceOf(DataArray);
    if (sumX instanceof DataArray) {
      expect(sumX.data).toEqual([6, 15]);
      expect(sumX.dims).toEqual(['y']);
    }
  });

  test('should compute total sum', () => {
    const data = [1, 2, 3, 4, 5];
    const da = new DataArray(data, { dims: ['x'] });

    const total = da.sum();
    expect(total).toBe(15);
  });

  test('should compute mean along dimension', () => {
    const data = [
      [1, 2, 3],
      [4, 5, 6]
    ];
    const da = new DataArray(data, {
      dims: ['y', 'x']
    });

    const meanX = da.mean('x');
    expect(meanX).toBeInstanceOf(DataArray);
    if (meanX instanceof DataArray) {
      expect(meanX.data).toEqual([2, 5]);
      expect(meanX.dims).toEqual(['y']);
    }
  });

  test('should compute total mean', () => {
    const data = [1, 2, 3, 4, 5];
    const da = new DataArray(data, { dims: ['x'] });

    const mean = da.mean();
    expect(mean).toBe(3);
  });

  test('should handle attributes', () => {
    const data = [1, 2, 3];
    const attrs = { units: 'meters', description: 'Test data' };
    const da = new DataArray(data, {
      dims: ['x'],
      attrs
    });

    expect(da.attrs).toEqual(attrs);
  });

  test('should handle name', () => {
    const data = [1, 2, 3];
    const da = new DataArray(data, {
      dims: ['x'],
      name: 'temperature'
    });

    expect(da.name).toBe('temperature');
  });

  test('should convert to object', () => {
    const data = [1, 2, 3];
    const da = new DataArray(data, {
      dims: ['x'],
      coords: { x: [0, 1, 2] },
      attrs: { units: 'meters' },
      name: 'test'
    });

    const obj = da.toObject();
    expect(obj.data).toEqual(data);
    expect(obj.dims).toEqual(['x']);
    expect(obj.coords).toEqual({ x: [0, 1, 2] });
    expect(obj.attrs).toEqual({ units: 'meters' });
    expect(obj.name).toBe('test');
  });

  test('should throw error for mismatched dimensions and data', () => {
    const data = [1, 2, 3];
    expect(() => {
      new DataArray(data, { dims: ['x', 'y'] });
    }).toThrow();
  });

  test('should throw error for mismatched coordinate length', () => {
    const data = [1, 2, 3];
    expect(() => {
      new DataArray(data, {
        dims: ['x'],
        coords: { x: [0, 1] } // Wrong length
      });
    }).toThrow();
  });
});
