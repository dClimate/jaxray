/**
 * Tests for Dataset
 */

import { strict as assert } from 'assert';
import { test, describe } from 'node:test';
import { DataArray } from './DataArray';
import { Dataset } from './Dataset';

describe('Dataset', () => {
  test('should create an empty dataset', () => {
    const ds = new Dataset();

    assert.deepEqual(ds.dataVars, []);
    assert.deepEqual(ds.dims, []);
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

    assert.deepEqual(ds.dataVars, ['temperature', 'pressure']);
    assert.deepEqual(ds.dims, ['x']);
  });

  test('should get a data variable', () => {
    const temp = new DataArray([1, 2, 3], { dims: ['x'] });
    const ds = new Dataset({ temperature: temp });

    const retrieved = ds.getVariable('temperature');
    assert.ok(retrieved);
    assert.deepEqual(retrieved.data, [1, 2, 3]);
  });

  test('should add a data variable', () => {
    const ds = new Dataset();
    const temp = new DataArray([1, 2, 3], { dims: ['x'] });

    ds.addVariable('temperature', temp);

    assert.ok(ds.hasVariable('temperature'));
    assert.deepEqual(ds.dataVars, ['temperature']);
  });

  test('should remove a data variable', () => {
    const temp = new DataArray([1, 2, 3], { dims: ['x'] });
    const ds = new Dataset({ temperature: temp });

    const removed = ds.removeVariable('temperature');

    assert.equal(removed, true);
    assert.equal(ds.hasVariable('temperature'), false);
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
    assert.equal(sizes.x, 3);
    assert.equal(sizes.y, 2);
  });

  test('should select data using sel()', () => {
    const temp = new DataArray([1, 2, 3, 4, 5], {
      dims: ['x'],
      coords: { x: [10, 20, 30, 40, 50] }
    });
    const ds = new Dataset({ temperature: temp });

    const selected = ds.sel({ x: 30 });
    const selectedTemp = selected.getVariable('temperature');

    assert.ok(selectedTemp);
    assert.equal(selectedTemp.data, 3);
  });

  test('should select data using isel()', () => {
    const temp = new DataArray([1, 2, 3, 4, 5], { dims: ['x'] });
    const ds = new Dataset({ temperature: temp });

    const selected = ds.isel({ x: 2 });
    const selectedTemp = selected.getVariable('temperature');

    assert.ok(selectedTemp);
    assert.equal(selectedTemp.data, 3);
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

    assert.ok(doubledTemp);
    assert.ok(doubledPressure);
    assert.deepEqual(doubledTemp.data, [2, 4, 6]);
    assert.deepEqual(doubledPressure.data, [200, 400, 600]);
  });

  test('should merge two datasets', () => {
    const temp = new DataArray([1, 2, 3], { dims: ['x'] });
    const pressure = new DataArray([100, 200, 300], { dims: ['x'] });

    const ds1 = new Dataset({ temperature: temp });
    const ds2 = new Dataset({ pressure: pressure });

    const merged = ds1.merge(ds2);

    assert.deepEqual(merged.dataVars.sort(), ['pressure', 'temperature']);
  });

  test('should throw error when merging datasets with duplicate variables', () => {
    const temp1 = new DataArray([1, 2, 3], { dims: ['x'] });
    const temp2 = new DataArray([4, 5, 6], { dims: ['x'] });

    const ds1 = new Dataset({ temperature: temp1 });
    const ds2 = new Dataset({ temperature: temp2 });

    assert.throws(() => {
      ds1.merge(ds2);
    });
  });

  test('should handle attributes', () => {
    const attrs = { description: 'Test dataset', version: '1.0' };
    const ds = new Dataset({}, { attrs });

    assert.deepEqual(ds.attrs, attrs);
  });

  test('should convert to object', () => {
    const temp = new DataArray([1, 2, 3], { dims: ['x'] });
    const ds = new Dataset(
      { temperature: temp },
      { attrs: { description: 'Test' } }
    );

    const obj = ds.toObject();

    assert.ok(obj.dataVars);
    assert.ok(obj.dataVars.temperature);
    assert.deepEqual(obj.attrs, { description: 'Test' });
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

    assert.deepEqual(ds.dims.sort(), ['x', 'y']);
  });

  test('should throw error for inconsistent dimension sizes', () => {
    const temp1 = new DataArray([1, 2, 3], { dims: ['x'] });
    const temp2 = new DataArray([4, 5], { dims: ['x'] });

    const ds = new Dataset({ temperature: temp1 });

    assert.throws(() => {
      ds.addVariable('pressure', temp2);
    });
  });
});
