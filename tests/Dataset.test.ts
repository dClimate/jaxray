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

  test('should select data using sel()', () => {
    const temp = new DataArray([1, 2, 3, 4, 5], {
      dims: ['x'],
      coords: { x: [10, 20, 30, 40, 50] }
    });
    const ds = new Dataset({ temperature: temp });

    const selected = ds.sel({ x: 30 });
    const selectedTemp = selected.getVariable('temperature');

    expect(selectedTemp).toBeTruthy();
    expect(selectedTemp?.data).toBe(3);
  });

  test('should select data using isel()', () => {
    const temp = new DataArray([1, 2, 3, 4, 5], { dims: ['x'] });
    const ds = new Dataset({ temperature: temp });

    const selected = ds.isel({ x: 2 });
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
});
