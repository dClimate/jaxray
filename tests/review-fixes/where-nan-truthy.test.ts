import { describe, test, expect } from 'vitest';
import { DataArray } from '../../src/DataArray';

describe('where() uses numpy truthiness for non-boolean conditions', () => {
  test('treats NaN as truthy in a 1-D condition with the default null fallback', () => {
    const values = new DataArray([10, 20], {
      dims: ['x'],
      coords: { x: [0, 1] }
    });
    const condition = new DataArray([NaN, 0], {
      dims: ['x'],
      coords: { x: [0, 1] }
    });

    expect(values.where(condition).data).toEqual([10, null]);
  });

  test('treats NaN as truthy in a 2-D condition with a scalar fallback', () => {
    const values = new DataArray(
      [
        [10, 20],
        [30, 40]
      ],
      {
        dims: ['x', 'y'],
        coords: { x: [0, 1], y: ['a', 'b'] }
      }
    );
    const condition = new DataArray(
      [
        [NaN, 0],
        [1, NaN]
      ],
      {
        dims: ['x', 'y'],
        coords: { x: [0, 1], y: ['a', 'b'] }
      }
    );

    expect(values.where(condition, -1).data).toEqual([
      [10, -1],
      [30, 40]
    ]);
  });

  test('applies numpy truthiness to mixed numeric and missing conditions', () => {
    const values = new DataArray([10, 20, 30, 40, 50, 60], {
      dims: ['x'],
      coords: { x: [0, 1, 2, 3, 4, 5] }
    });
    const condition = new DataArray([NaN, 0, 1, -2, Infinity, null], {
      dims: ['x'],
      coords: { x: [0, 1, 2, 3, 4, 5] }
    });

    expect(values.where(condition, -1).data).toEqual([10, -1, 30, 40, 50, -1]);
  });

  test('keeps boolean conditions unchanged and treats undefined as missing', () => {
    const values = new DataArray([10, 20, 30, 40], {
      dims: ['x'],
      coords: { x: [0, 1, 2, 3] }
    });
    // undefined is supported at runtime as a missing condition but is not in DataValue's public type.
    const conditions = [true, false, null, undefined] as unknown as (boolean | null)[];
    const condition = new DataArray(conditions, {
      dims: ['x'],
      coords: { x: [0, 1, 2, 3] }
    });

    expect(values.where(condition).data).toEqual([10, null, null, null]);
  });
});
