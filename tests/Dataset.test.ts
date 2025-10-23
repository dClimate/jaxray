/**
 * Tests for Dataset
 */

import { describe, test, expect } from 'vitest';
import { DataArray } from '../src/DataArray';
import { Dataset } from '../src/Dataset';

describe('Dataset', () => {
  test('should create an empty dataset', () => {
    const ds = new Dataset();

    expect(ds.dataVars).toEqual([]);
    expect(ds.dims).toEqual([]);
  });

  test('should create a dataset with data variables', () => {
    const temp = new DataArray([1, 2, 3], {
      dims: ['x'],
      coords: { x: [0, 1, 2] }
    });
    const pressure = new DataArray([100, 200, 300], {
      dims: ['x'],
      coords: { x: [0, 1, 2] }
    });

    const ds = new Dataset({
      temperature: temp,
      pressure: pressure
    });

    expect(ds.dataVars).toEqual(['temperature', 'pressure']);
    expect(ds.dims).toEqual(['x']);
  });

  test('should get a data variable', () => {
    const temp = new DataArray([1, 2, 3], { dims: ['x'] });
    const ds = new Dataset({ temperature: temp });

    const retrieved = ds.getVariable('temperature');
    expect(retrieved).toBeTruthy();
    expect(retrieved?.data).toEqual([1, 2, 3]);
  });

  test('should add a data variable', () => {
    const ds = new Dataset();
    const temp = new DataArray([1, 2, 3], { dims: ['x'] });

    ds.addVariable('temperature', temp);

    expect(ds.hasVariable('temperature')).toBe(true);
    expect(ds.dataVars).toEqual(['temperature']);
  });

  test('should remove a data variable', () => {
    const temp = new DataArray([1, 2, 3], { dims: ['x'] });
    const ds = new Dataset({ temperature: temp });

    const removed = ds.removeVariable('temperature');

    expect(removed).toBe(true);
    expect(ds.hasVariable('temperature')).toBe(false);
  });

  test('should get dimension sizes', () => {
    const temp = new DataArray(
      [
        [1, 2, 3],
        [4, 5, 6]
      ],
      { dims: ['y', 'x'] }
    );
    const ds = new Dataset({ temperature: temp });

    const sizes = ds.sizes;
    expect(sizes.x).toBe(3);
    expect(sizes.y).toBe(2);
  });

  test('should select data using sel()', async () => {
    const temp = new DataArray([1, 2, 3, 4, 5], {
      dims: ['x'],
      coords: { x: [10, 20, 30, 40, 50] }
    });
    const ds = new Dataset({ temperature: temp });

    const selected = await ds.sel({ x: 30 });
    const selectedTemp = selected.getVariable('temperature');

    expect(selectedTemp).toBeTruthy();
    expect(selectedTemp?.data).toBe(3);
  });

  test('should select data using isel()', async () => {
    const temp = new DataArray([1, 2, 3, 4, 5], { dims: ['x'] });
    const ds = new Dataset({ temperature: temp });

    const selected = await ds.isel({ x: 2 });
    const selectedTemp = selected.getVariable('temperature');

    expect(selectedTemp).toBeTruthy();
    expect(selectedTemp?.data).toBe(3);
  });

  test('should map function over all variables', () => {
    const temp = new DataArray([1, 2, 3], { dims: ['x'] });
    const pressure = new DataArray([100, 200, 300], { dims: ['x'] });
    const ds = new Dataset({ temperature: temp, pressure: pressure });

    const doubled = ds.map((da) => {
      const newData = (da.data as number[]).map((v) => v * 2);
      return new DataArray(newData, { dims: da.dims });
    });

    const doubledTemp = doubled.getVariable('temperature');
    const doubledPressure = doubled.getVariable('pressure');

    expect(doubledTemp).toBeTruthy();
    expect(doubledPressure).toBeTruthy();
    expect(doubledTemp?.data).toEqual([2, 4, 6]);
    expect(doubledPressure?.data).toEqual([200, 400, 600]);
  });

  test('rename should rename data variables', () => {
    const temp = new DataArray([1, 2, 3], { dims: ['x'] });
    const pressure = new DataArray([100, 200, 300], { dims: ['x'] });
    const ds = new Dataset({ temperature: temp, pressure });

    const renamed = ds.rename({ temperature: 'temp_c' });

    expect(renamed.dataVars.sort()).toEqual(['pressure', 'temp_c']);
    expect(renamed.getVariable('temp_c').data).toEqual([1, 2, 3]);
    expect(() => renamed.getVariable('temperature')).toThrow();

    // Original dataset unchanged
    expect(ds.dataVars.sort()).toEqual(['pressure', 'temperature']);
  });

  test('assignCoords should update dataset and variables', () => {
    const temp = new DataArray([10, 11, 12], {
      dims: ['time'],
      coords: { time: [0, 1, 2] }
    });
    const ds = new Dataset({ temperature: temp });

    const assigned = ds.assignCoords({ time: ['2020-01-01', '2020-01-02', '2020-01-03'] });
    const tempAssigned = assigned.getVariable('temperature');

    expect(assigned.coords.time).toEqual(['2020-01-01', '2020-01-02', '2020-01-03']);
    expect(tempAssigned.coords.time).toEqual(['2020-01-01', '2020-01-02', '2020-01-03']);

    // original untouched
    expect(ds.coords.time).toEqual([0, 1, 2]);
  });

  test('assignCoords should accept DataArray values', () => {
    const temp = new DataArray([10, 11, 12], {
      dims: ['time'],
      coords: { time: [0, 1, 2] }
    });
    const ds = new Dataset({ temperature: temp });

    const newTime = new DataArray(['a', 'b', 'c'], {
      dims: ['time'],
      coords: { time: temp.coords.time }
    });

    const assigned = ds.assignCoords({ time: newTime });

    expect(assigned.coords.time).toEqual(['a', 'b', 'c']);
    expect(assigned.getVariable('temperature').coords.time).toEqual(['a', 'b', 'c']);
  });

  test('assignCoords should validate length', () => {
    const temp = new DataArray([10, 11, 12], {
      dims: ['time'],
      coords: { time: [0, 1, 2] }
    });
    const ds = new Dataset({ temperature: temp });

    expect(() => ds.assignCoords({ time: ['2020-01-01'] })).toThrow(
      /length/
    );
  });

  test('compute should materialize lazy Dataset variables', async () => {
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

    const lazyVar = new DataArray(null, {
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

    const ds = new Dataset({ temperature: lazyVar });

    expect(ds.getVariable('temperature').isLazy).toBe(true);

    const computed = await ds.compute();
    const computedTemp = computed.getVariable('temperature');

    expect(computedTemp.isLazy).toBe(false);
    expect(computedTemp.data).toEqual(raw);
    expect(computedTemp.attrs).toEqual({ units: 'C' });

    // Original dataset remains lazy
    expect(ds.getVariable('temperature').isLazy).toBe(true);
  });

  test('should apply where across dataset variables', () => {
    const temp = new DataArray([1, 2, 3], {
      dims: ['x'],
      coords: { x: [0, 1, 2] }
    });
    const humidity = new DataArray([50, 60, 70], {
      dims: ['x'],
      coords: { x: [0, 1, 2] }
    });
    const ds = new Dataset({ temperature: temp, humidity });

    const cond = new DataArray([true, false, true], {
      dims: ['x'],
      coords: { x: [0, 1, 2] }
    });

    const replacements = new Dataset({
      temperature: new DataArray([-1, -1, -1], {
        dims: ['x'],
        coords: { x: [0, 1, 2] }
      }),
      humidity: new DataArray([0, 0, 0], {
        dims: ['x'],
        coords: { x: [0, 1, 2] }
      })
    });

    const masked = ds.where(cond, replacements);

    const maskedTemp = masked.getVariable('temperature');
    const maskedHumidity = masked.getVariable('humidity');

    expect(maskedTemp?.data).toEqual([1, -1, 3]);
    expect(maskedHumidity?.data).toEqual([50, 0, 70]);
    expect(maskedTemp?.dims).toEqual(['x']);
    expect(maskedHumidity?.coords['x']).toEqual([0, 1, 2]);
  });

  test('should merge two datasets', () => {
    const temp = new DataArray([1, 2, 3], { dims: ['x'] });
    const pressure = new DataArray([100, 200, 300], { dims: ['x'] });

    const ds1 = new Dataset({ temperature: temp });
    const ds2 = new Dataset({ pressure: pressure });

    const merged = ds1.merge(ds2);

    expect(merged.dataVars.sort()).toEqual(['pressure', 'temperature']);
  });

  test('should throw error when merging datasets with duplicate variables', () => {
    const temp1 = new DataArray([1, 2, 3], { dims: ['x'] });
    const temp2 = new DataArray([4, 5, 6], { dims: ['x'] });

    const ds1 = new Dataset({ temperature: temp1 });
    const ds2 = new Dataset({ temperature: temp2 });

    expect(() => {
      ds1.merge(ds2);
    }).toThrow();
  });

  test('should handle attributes', () => {
    const attrs = { description: 'Test dataset', version: '1.0' };
    const ds = new Dataset({}, { attrs });

    expect(ds.attrs).toEqual(attrs);
  });

  test('should convert to object', () => {
    const temp = new DataArray([1, 2, 3], { dims: ['x'] });
    const ds = new Dataset(
      { temperature: temp },
      { attrs: { description: 'Test' } }
    );

    const obj = ds.toObject();

    expect(obj.dataVars).toBeTruthy();
    expect(obj.dataVars?.temperature).toBeTruthy();
    expect(obj.attrs).toEqual({ description: 'Test' });
  });

  test('should handle multiple dimensions', () => {
    const temp = new DataArray(
      [
        [1, 2, 3],
        [4, 5, 6]
      ],
      { dims: ['y', 'x'] }
    );
    const pressure = new DataArray([100, 200], { dims: ['y'] });

    const ds = new Dataset({
      temperature: temp,
      pressure: pressure
    });

    expect(ds.dims.sort()).toEqual(['x', 'y']);
  });

  test('should throw error for inconsistent dimension sizes', () => {
    const temp1 = new DataArray([1, 2, 3], { dims: ['x'] });
    const temp2 = new DataArray([4, 5], { dims: ['x'] });

    const ds = new Dataset({ temperature: temp1 });

    expect(() => {
      ds.addVariable('pressure', temp2);
    }).toThrow();
  });

  describe('Selection methods', () => {
    test('should select with nearest neighbor method', async () => {
      const temp = new DataArray([10, 20, 30, 40, 50], {
        dims: ['x'],
        coords: {
          x: [0, 5, 10, 15, 20]
        }
      });
      const pressure = new DataArray([100, 200, 300, 400, 500], {
        dims: ['x'],
        coords: {
          x: [0, 5, 10, 15, 20]
        }
      });

      const ds = new Dataset({ temperature: temp, pressure: pressure });

      // Select nearest to 7 (should be index with x=5)
      const selected = await ds.sel({ x: 7 }, { method: 'nearest' });
      expect(selected.getVariable('temperature').data).toBe(20);
      expect(selected.getVariable('pressure').data).toBe(200);
    });

    test('should select with tolerance', async () => {
      const temp = new DataArray([10, 20, 30], {
        dims: ['x'],
        coords: {
          x: [0, 10, 20]
        }
      });

      const ds = new Dataset({ temperature: temp });

      // Should work
      const selected1 = await ds.sel({ x: 13 }, { method: 'nearest', tolerance: 5 });
      expect(selected1.getVariable('temperature').data).toBe(20);

      // Should fail
      await expect(
        ds.sel({ x: 13 }, { method: 'nearest', tolerance: 2 })
      ).rejects.toThrow('No coordinate within tolerance');
    });

    test('should select with ffill method', async () => {
      const temp = new DataArray([10, 20, 30, 40], {
        dims: ['x'],
        coords: {
          x: [0, 5, 10, 15]
        }
      });

      const ds = new Dataset({ temperature: temp });

      const selected = await ds.sel({ x: 7 }, { method: 'ffill' });
      expect(selected.getVariable('temperature').data).toBe(20);
    });

    test('should select with bfill method', async () => {
      const temp = new DataArray([10, 20, 30, 40], {
        dims: ['x'],
        coords: {
          x: [0, 5, 10, 15]
        }
      });

      const ds = new Dataset({ temperature: temp });

      const selected = await ds.sel({ x: 7 }, { method: 'bfill' });
      expect(selected.getVariable('temperature').data).toBe(30);
    });

    test('should work with multi-dimensional data', async () => {
      const temp = new DataArray(
        [
          [1, 2, 3],
          [4, 5, 6],
          [7, 8, 9]
        ],
        {
          dims: ['y', 'x'],
          coords: {
            y: [0, 10, 20],
            x: [0, 5, 10]
          }
        }
      );

      const ds = new Dataset({ temperature: temp });

      const selected = await ds.sel(
        { y: 8, x: 6 },
        { method: 'nearest' }
      );
      expect(selected.getVariable('temperature').data).toBe(5);
    });

    test('should apply method to relevant dimensions only', async () => {
      const temp = new DataArray(
        [
          [1, 2, 3],
          [4, 5, 6]
        ],
        {
          dims: ['y', 'x'],
          coords: {
            y: [0, 10],
            x: [0, 5, 10]
          }
        }
      );
      const pressure = new DataArray([100, 200], {
        dims: ['y'],
        coords: {
          y: [0, 10]
        }
      });

      const ds = new Dataset({ temperature: temp, pressure: pressure });

      // Select with nearest on both dimensions
      // y=8 nearest to 10, x=3 nearest to 5
      const selected = await ds.sel({ y: 8, x: 3 }, { method: 'nearest' });
      expect(selected.getVariable('temperature').data).toBe(5);
      expect(selected.getVariable('pressure').data).toBe(200);
    });
  });

  describe('encryption detection', () => {
    test('should detect no encryption for dataset without codecs', () => {
      const temp = new DataArray([1, 2, 3], {
        dims: ['x'],
        coords: { x: [0, 1, 2] }
      });

      const ds = new Dataset({ temperature: temp });
      expect(ds.detectEncryption()).toBe(false);
      expect(ds.isEncrypted).toBe(false);
    });

    test('should detect no encryption for dataset with non-encrypted codecs', () => {
      const temp = new DataArray([1, 2, 3], {
        dims: ['x'],
        coords: { x: [0, 1, 2] },
        attrs: {
          codecs: [
            { name: 'bytes', configuration: { endian: 'little' } },
            { name: 'gzip', configuration: { level: 5 } }
          ]
        }
      });

      const ds = new Dataset({ temperature: temp });
      expect(ds.detectEncryption()).toBe(false);
      expect(ds.isEncrypted).toBe(false);
    });

    test('should detect encryption with xchacha20poly1305 codec', () => {
      const temp = new DataArray([1, 2, 3], {
        dims: ['x'],
        coords: { x: [0, 1, 2] },
        attrs: {
          codecs: [
            { name: 'bytes', configuration: { endian: 'little' } },
            { name: 'xchacha20poly1305', configuration: { key: 'encrypted' } }
          ]
        }
      });

      const ds = new Dataset({ temperature: temp });
      expect(ds.detectEncryption()).toBe(true);
      expect(ds.isEncrypted).toBe(true);
    });

    test('should detect encryption in any data variable', () => {
      const temp = new DataArray([1, 2, 3], {
        dims: ['x'],
        coords: { x: [0, 1, 2] },
        attrs: {
          codecs: [{ name: 'gzip' }]
        }
      });

      const pressure = new DataArray([100, 200, 300], {
        dims: ['x'],
        coords: { x: [0, 1, 2] },
        attrs: {
          codecs: [
            { name: 'bytes' },
            { name: 'xchacha20poly1305' }
          ]
        }
      });

      const ds = new Dataset({ temperature: temp, pressure: pressure });
      expect(ds.detectEncryption()).toBe(true);
      expect(ds.isEncrypted).toBe(true);
    });

    test('should handle missing codecs array', () => {
      const temp = new DataArray([1, 2, 3], {
        dims: ['x'],
        coords: { x: [0, 1, 2] },
        attrs: {}
      });

      const ds = new Dataset({ temperature: temp });
      expect(ds.detectEncryption()).toBe(false);
      expect(ds.isEncrypted).toBe(false);
    });

    test('should handle empty codecs array', () => {
      const temp = new DataArray([1, 2, 3], {
        dims: ['x'],
        coords: { x: [0, 1, 2] },
        attrs: {
          codecs: []
        }
      });

      const ds = new Dataset({ temperature: temp });
      expect(ds.detectEncryption()).toBe(false);
      expect(ds.isEncrypted).toBe(false);
    });

    test('should handle codecs without name property', () => {
      const temp = new DataArray([1, 2, 3], {
        dims: ['x'],
        coords: { x: [0, 1, 2] },
        attrs: {
          codecs: [
            { type: 'bytes' },
            { configuration: {} }
          ]
        }
      });

      const ds = new Dataset({ temperature: temp });
      expect(ds.detectEncryption()).toBe(false);
      expect(ds.isEncrypted).toBe(false);
    });

    test('should reset encryption status when re-detecting', () => {
      const tempEncrypted = new DataArray([1, 2, 3], {
        dims: ['x'],
        coords: { x: [0, 1, 2] },
        attrs: {
          codecs: [{ name: 'xchacha20poly1305' }]
        }
      });

      const ds = new Dataset({ temperature: tempEncrypted });
      expect(ds.detectEncryption()).toBe(true);
      expect(ds.isEncrypted).toBe(true);

      // Remove the encrypted variable
      ds.removeVariable('temperature');

      // Add a non-encrypted variable
      const tempPlain = new DataArray([4, 5, 6], {
        dims: ['x'],
        coords: { x: [0, 1, 2] },
        attrs: {
          codecs: [{ name: 'gzip' }]
        }
      });
      ds.addVariable('temperature', tempPlain);

      // Re-detect should now return false
      expect(ds.detectEncryption()).toBe(false);
      expect(ds.isEncrypted).toBe(false);
    });
  });
});
