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

  test('should select data by label using sel()', async () => {
    const data = [1, 2, 3, 4, 5];
    const da = new DataArray(data, {
      dims: ['x'],
      coords: {
        x: [10, 20, 30, 40, 50]
      }
    });

    const selected = await da.sel({ x: 30 });
    expect(selected.data).toBe(3);
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

  test('should apply where with scalar fallback', () => {
    const x = new DataArray([0.1, 0.6, 0.4, 0.8], {
      dims: ['time'],
      coords: { time: [0, 1, 2, 3] },
      attrs: { units: 'degC' },
      name: 'sst'
    });
    const cond = new DataArray([true, false, true, false], {
      dims: ['time'],
      coords: { time: [0, 1, 2, 3] }
    });

    const masked = x.where(cond, -1, { keepAttrs: true });

    expect(masked.data).toEqual([0.1, -1, 0.4, -1]);
    expect(masked.dims).toEqual(['time']);
    expect(masked.coords['time']).toEqual([0, 1, 2, 3]);
    expect(masked.name).toBe('sst');
    expect(masked.attrs).toEqual({ units: 'degC' });
  });

  test('should broadcast where across distinct dimensions', () => {
    const cond = new DataArray([true, false], {
      dims: ['x'],
      coords: { x: [0, 1] }
    });
    const values = new DataArray([1, 2], {
      dims: ['y'],
      coords: { y: ['a', 'b'] }
    });

    const result = DataArray.where(cond, values, 0);

    expect(result.dims).toEqual(['x', 'y']);
    expect(result.coords['x']).toEqual([0, 1]);
    expect(result.coords['y']).toEqual(['a', 'b']);
    expect(result.data).toEqual([
      [1, 2],
      [0, 0]
    ]);
  });

  test('should support arithmetic and comparison helpers for where usage', () => {
    const x = new DataArray(
      [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9],
      {
        dims: ['lat'],
        coords: { lat: Array.from({ length: 10 }, (_, i) => i) },
        name: 'sst'
      }
    );

    const result = DataArray.where(x.lt(0.5), x, x.mul(100));

    expect(result.dims).toEqual(['lat']);
    expect(result.coords['lat']).toEqual(Array.from({ length: 10 }, (_, i) => i));
    expect(result.data).toEqual([0, 0.1, 0.2, 0.3, 0.4, 50, 60, 70, 80, 90]);
    expect(result.name).toBe('sst');
  });

  test('lt should return boolean DataArray', () => {
    const x = new DataArray([1, 2, 3], {
      dims: ['x'],
      coords: { x: [0, 1, 2] }
    });

    const mask = x.lt(2);

    expect(mask.data).toEqual([true, false, false]);
    expect(mask.dims).toEqual(['x']);
    expect(mask.coords['x']).toEqual([0, 1, 2]);
  });

  test('multiply should keep attrs and name by default', () => {
    const x = new DataArray([1, 2, 3], {
      dims: ['x'],
      coords: { x: [0, 1, 2] },
      attrs: { units: 'm' },
      name: 'distance'
    });

    const scaled = x.mul(10);

    expect(scaled.data).toEqual([10, 20, 30]);
    expect(scaled.attrs).toEqual({ units: 'm' });
    expect(scaled.name).toBe('distance');
  });

  test('compute should materialize lazy DataArray', async () => {
    const raw = [
      [1, 2],
      [3, 4]
    ];

    const loader = async (ranges: { [dim: string]: { start: number; stop: number } | number }) => {
      const sliceAxis = (values: number[], range: { start: number; stop: number } | number) => {
        if (typeof range === 'number') {
          return [values[range]];
        }
        const start = range?.start ?? 0;
        const stop = range?.stop ?? values.length;
        return values.slice(start, stop);
      };

      const xRange = ranges.x ?? { start: 0, stop: raw.length };
      const yRange = ranges.y ?? { start: 0, stop: raw[0].length };

      const rows = typeof xRange === 'number'
        ? [raw[xRange]]
        : raw.slice(xRange.start ?? 0, xRange.stop ?? raw.length);

      const result = rows.map(row => {
        const slice = sliceAxis(row, yRange);
        return slice;
      });

      if (typeof xRange === 'number' && typeof yRange === 'number') {
        return result[0][0];
      }
      if (typeof xRange === 'number') {
        return result[0];
      }
      if (typeof yRange === 'number') {
        return result.map(row => row[0]);
      }
      return result;
    };

    const lazy = new DataArray(null, {
      lazy: true,
      virtualShape: [2, 2],
      lazyLoader: loader,
      dims: ['x', 'y'],
      coords: {
        x: [0, 1],
        y: [0, 1]
      },
      attrs: { units: 'C' },
      name: 'temp'
    });

    expect(lazy.isLazy).toBe(true);
    expect(() => lazy.data).toThrow('Materializing a lazy DataBlock requires an explicit execution step.');

    const computed = await lazy.compute();

    expect(computed).not.toBe(lazy);
    expect(computed.isLazy).toBe(false);
    expect(computed.data).toEqual(raw);
    expect(computed.attrs).toEqual({ units: 'C' });
    expect(computed.name).toBe('temp');
  });

  test('assignCoords should update coordinates with arrays', () => {
    const da = new DataArray([1, 2, 3], {
      dims: ['lon'],
      coords: { lon: [-170, -160, -150] }
    });

    const assigned = da.assignCoords({ lon: [-180, -170, -160] });

    expect(assigned.coords.lon).toEqual([-180, -170, -160]);
    expect(da.coords.lon).toEqual([-170, -160, -150]);
  });

  test('assignCoords should accept DataArray values', () => {
    const da = new DataArray([1, 2, 3], {
      dims: ['lon'],
      coords: { lon: [-190, 170, 200] }
    });

    const adjustedValues = da.coords.lon.map(value => (((value + 180) % 360) + 360) % 360 - 180);
    const adjustedDA = new DataArray(adjustedValues, {
      dims: ['lon'],
      coords: { lon: da.coords.lon }
    });

    const assigned = da.assignCoords({ lon: adjustedDA });

    expect(assigned.coords.lon).toEqual(adjustedValues);
    expect(da.coords.lon).toEqual([-190, 170, 200]);
  });

  test('squeeze should drop singleton dimensions', () => {
    const da = new DataArray([
      [1],
      [2]
    ], {
      dims: ['x', 'y'],
      coords: {
        x: [0, 1],
        y: [0]
      }
    });

    const squeezed = da.squeeze();

    expect(squeezed.dims).toEqual(['x']);
    expect(squeezed.data).toEqual([1, 2]);
    expect(squeezed.coords.x).toEqual([0, 1]);
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

  describe('Selection methods', () => {
    test('should select nearest neighbor', async () => {
      const data = [10, 20, 30, 40, 50];
      const da = new DataArray(data, {
        dims: ['x'],
        coords: {
          x: [0, 5, 10, 15, 20]
        }
      });

      // Select nearest to 7 (should be 5)
      const selected = await da.sel({ x: 7 }, { method: 'nearest' });
      expect(selected.data).toBe(20);

      // Select nearest to 13 (should be 15)
      const selected2 = await da.sel({ x: 13 }, { method: 'nearest' });
      expect(selected2.data).toBe(40);

      // Select nearest to 2.4 (should be 0, distance 2.4)
      const selected3 = await da.sel({ x: 2.4 }, { method: 'nearest' });
      expect(selected3.data).toBe(10);
    });

    test('should use tolerance with nearest neighbor', async () => {
      const data = [10, 20, 30, 40, 50];
      const da = new DataArray(data, {
        dims: ['x'],
        coords: {
          x: [0, 5, 10, 15, 20]
        }
      });

      // This should work (distance = 2)
      const selected = await da.sel({ x: 7 }, { method: 'nearest', tolerance: 3 });
      expect(selected.data).toBe(20);

      // This should fail (distance = 7)
      await expect(
        da.sel({ x: 7 }, { method: 'nearest', tolerance: 1 })
      ).rejects.toThrow('No coordinate within tolerance');
    });

    test('should forward fill (ffill/pad)', async () => {
      const data = [10, 20, 30, 40, 50];
      const da = new DataArray(data, {
        dims: ['x'],
        coords: {
          x: [0, 5, 10, 15, 20]
        }
      });

      // Select last value <= 7 (should be 5)
      const selected1 = await da.sel({ x: 7 }, { method: 'ffill' });
      expect(selected1.data).toBe(20);

      // Select last value <= 12 (should be 10)
      const selected2 = await da.sel({ x: 12 }, { method: 'pad' });
      expect(selected2.data).toBe(30);

      // Select exact match
      const selected3 = await da.sel({ x: 10 }, { method: 'ffill' });
      expect(selected3.data).toBe(30);

      // Should fail if no value <= target
      await expect(
        da.sel({ x: -5 }, { method: 'ffill' })
      ).rejects.toThrow('No coordinate <= -5');
    });

    test('should backward fill (bfill/backfill)', async () => {
      const data = [10, 20, 30, 40, 50];
      const da = new DataArray(data, {
        dims: ['x'],
        coords: {
          x: [0, 5, 10, 15, 20]
        }
      });

      // Select first value >= 7 (should be 10)
      const selected1 = await da.sel({ x: 7 }, { method: 'bfill' });
      expect(selected1.data).toBe(30);

      // Select first value >= 12 (should be 15)
      const selected2 = await da.sel({ x: 12 }, { method: 'backfill' });
      expect(selected2.data).toBe(40);

      // Select exact match
      const selected3 = await da.sel({ x: 10 }, { method: 'bfill' });
      expect(selected3.data).toBe(30);

      // Should fail if no value >= target
      await expect(
        da.sel({ x: 25 }, { method: 'bfill' })
      ).rejects.toThrow('No coordinate >= 25');
    });

    test('should work with multiple dimensions', async () => {
      const data = [
        [1, 2, 3],
        [4, 5, 6],
        [7, 8, 9]
      ];
      const da = new DataArray(data, {
        dims: ['y', 'x'],
        coords: {
          y: [0, 10, 20],
          x: [0, 5, 10]
        }
      });

      // Select using nearest on both dimensions
      const selected = await da.sel(
        { y: 8, x: 6 },
        { method: 'nearest' }
      );
      expect(selected.data).toBe(5);

      // Mixed: exact on one, nearest on other
      const selected2 = await da.sel(
        { y: 10, x: 7 },
        { method: 'nearest' }
      );
      expect(selected2.data).toBe(5);
    });

    test('should throw error for non-numeric coordinates with nearest', async () => {
      const data = [1, 2, 3];
      const da = new DataArray(data, {
        dims: ['x'],
        coords: {
          x: ['a', 'b', 'c']
        }
      });

      await expect(
        da.sel({ x: 'x' }, { method: 'nearest' })
      ).rejects.toThrow('Nearest neighbor lookup requires numeric coordinates');
    });

    test('should work with array selections using method', async () => {
      const data = [10, 20, 30, 40, 50];
      const da = new DataArray(data, {
        dims: ['x'],
        coords: {
          x: [0, 5, 10, 15, 20]
        }
      });

      // Select multiple values with nearest
      // 3 -> nearest is 5 (index 1)
      // 8 -> nearest is 10 (index 2)
      // 17 -> nearest is 15 (index 3)
      // Array selection creates a range from min to max index
      const selected = await da.sel({ x: [3, 8, 17] }, { method: 'nearest' });
      expect(selected.data).toEqual([20, 30, 40]);
    });

    test('should apply tolerance to ffill', async () => {
      const data = [10, 20, 30];
      const da = new DataArray(data, {
        dims: ['x'],
        coords: {
          x: [0, 10, 20]
        }
      });

      // Should work (distance = 3)
      const selected1 = await da.sel({ x: 13 }, { method: 'ffill', tolerance: 5 });
      expect(selected1.data).toBe(20);

      // Should fail (distance = 13)
      await expect(
        da.sel({ x: 13 }, { method: 'ffill', tolerance: 2 })
      ).rejects.toThrow('No coordinate within tolerance');
    });

    test('should apply tolerance to bfill', async () => {
      const data = [10, 20, 30];
      const da = new DataArray(data, {
        dims: ['x'],
        coords: {
          x: [0, 10, 20]
        }
      });

      // Should work (distance = 7)
      const selected1 = await da.sel({ x: 13 }, { method: 'bfill', tolerance: 10 });
      expect(selected1.data).toBe(30);

      // Should fail (distance = 7)
      await expect(
        da.sel({ x: 13 }, { method: 'bfill', tolerance: 5 })
      ).rejects.toThrow('No coordinate within tolerance');
    });
  });
});
