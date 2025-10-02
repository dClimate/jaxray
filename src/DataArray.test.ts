/**
 * Tests for DataArray
 */

import { strict as assert } from 'assert';
import { test, describe } from 'node:test';
import { DataArray } from './DataArray';

describe('DataArray', () => {
  test('should create a simple 1D DataArray', () => {
    const data = [1, 2, 3, 4, 5];
    const da = new DataArray(data, { dims: ['x'] });

    assert.deepEqual(da.data, data);
    assert.deepEqual(da.dims, ['x']);
    assert.deepEqual(da.shape, [5]);
    assert.equal(da.ndim, 1);
    assert.equal(da.size, 5);
  });

  test('should create a 2D DataArray', () => {
    const data = [
      [1, 2, 3],
      [4, 5, 6]
    ];
    const da = new DataArray(data, {
      dims: ['y', 'x']
    });

    assert.deepEqual(da.data, data);
    assert.deepEqual(da.dims, ['y', 'x']);
    assert.deepEqual(da.shape, [2, 3]);
    assert.equal(da.ndim, 2);
    assert.equal(da.size, 6);
  });

  test('should create DataArray with custom coordinates', () => {
    const data = [1, 2, 3];
    const da = new DataArray(data, {
      dims: ['time'],
      coords: {
        time: ['2021-01-01', '2021-01-02', '2021-01-03']
      }
    });

    assert.deepEqual(da.coords['time'], ['2021-01-01', '2021-01-02', '2021-01-03']);
  });

  test('should auto-generate dimension names if not provided', () => {
    const data = [[1, 2], [3, 4]];
    const da = new DataArray(data);

    assert.deepEqual(da.dims, ['dim_0', 'dim_1']);
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
    assert.equal(selected.data, 3);
  });

  test('should select multiple values using sel()', () => {
    const data = [1, 2, 3, 4, 5];
    const da = new DataArray(data, {
      dims: ['x'],
      coords: {
        x: [10, 20, 30, 40, 50]
      }
    });

    const selected = da.sel({ x: [10, 30, 50] });
    assert.deepEqual(selected.data, [1, 3, 5]);
  });

  test('should slice data using sel()', () => {
    const data = [1, 2, 3, 4, 5];
    const da = new DataArray(data, {
      dims: ['x'],
      coords: {
        x: [10, 20, 30, 40, 50]
      }
    });

    const selected = da.sel({ x: { start: 20, stop: 40 } });
    assert.deepEqual(selected.data, [2, 3, 4]);
  });

  test('should select by integer position using isel()', () => {
    const data = [1, 2, 3, 4, 5];
    const da = new DataArray(data, { dims: ['x'] });

    const selected = da.isel({ x: 2 });
    assert.equal(selected.data, 3);
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
    assert.ok(sumX instanceof DataArray);
    assert.deepEqual(sumX.data, [6, 15]);
    assert.deepEqual(sumX.dims, ['y']);
  });

  test('should compute total sum', () => {
    const data = [1, 2, 3, 4, 5];
    const da = new DataArray(data, { dims: ['x'] });

    const total = da.sum();
    assert.equal(total, 15);
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
    assert.ok(meanX instanceof DataArray);
    assert.deepEqual(meanX.data, [2, 5]);
    assert.deepEqual(meanX.dims, ['y']);
  });

  test('should compute total mean', () => {
    const data = [1, 2, 3, 4, 5];
    const da = new DataArray(data, { dims: ['x'] });

    const mean = da.mean();
    assert.equal(mean, 3);
  });

  test('should handle attributes', () => {
    const data = [1, 2, 3];
    const attrs = { units: 'meters', description: 'Test data' };
    const da = new DataArray(data, {
      dims: ['x'],
      attrs
    });

    assert.deepEqual(da.attrs, attrs);
  });

  test('should handle name', () => {
    const data = [1, 2, 3];
    const da = new DataArray(data, {
      dims: ['x'],
      name: 'temperature'
    });

    assert.equal(da.name, 'temperature');
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
    assert.deepEqual(obj.data, data);
    assert.deepEqual(obj.dims, ['x']);
    assert.deepEqual(obj.coords, { x: [0, 1, 2] });
    assert.deepEqual(obj.attrs, { units: 'meters' });
    assert.equal(obj.name, 'test');
  });

  test('should throw error for mismatched dimensions and data', () => {
    const data = [1, 2, 3];
    assert.throws(() => {
      new DataArray(data, { dims: ['x', 'y'] });
    });
  });

  test('should throw error for mismatched coordinate length', () => {
    const data = [1, 2, 3];
    assert.throws(() => {
      new DataArray(data, {
        dims: ['x'],
        coords: { x: [0, 1] } // Wrong length
      });
    });
  });
});
