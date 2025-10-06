/**
 * Tests for utility functions
 */

import { describe, test, expect } from 'vitest';
import {
  getShape,
  flatten,
  reshape,
  getAtIndex,
  setAtIndex,
  deepClone,
  arraysEqual
} from '../src/utils';

describe('getShape', () => {
  test('should return empty array for scalar', () => {
    expect(getShape(5)).toEqual([]);
  });

  test('should return shape for 1D array', () => {
    expect(getShape([1, 2, 3, 4, 5])).toEqual([5]);
  });

  test('should return shape for 2D array', () => {
    expect(getShape([[1, 2, 3], [4, 5, 6]])).toEqual([2, 3]);
  });

  test('should return shape for 3D array', () => {
    const data = [
      [[1, 2], [3, 4]],
      [[5, 6], [7, 8]]
    ];
    expect(getShape(data)).toEqual([2, 2, 2]);
  });

  test('should handle empty array', () => {
    expect(getShape([])).toEqual([0]);
  });
});

describe('flatten', () => {
  test('should return array with scalar', () => {
    expect(flatten(5)).toEqual([5]);
  });

  test('should flatten 1D array', () => {
    expect(flatten([1, 2, 3])).toEqual([1, 2, 3]);
  });

  test('should flatten 2D array', () => {
    expect(flatten([[1, 2], [3, 4]])).toEqual([1, 2, 3, 4]);
  });

  test('should flatten 3D array', () => {
    const data = [
      [[1, 2], [3, 4]],
      [[5, 6], [7, 8]]
    ];
    expect(flatten(data)).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
  });

  test('should flatten irregular array', () => {
    const result = flatten([1, [2, 3], [[4, 5]]] as any);
    expect(result).toEqual([1, 2, 3, 4, 5]);
  });
});

describe('reshape', () => {
  test('should return scalar for empty shape', () => {
    expect(reshape([5], [])).toBe(5);
  });

  test('should return same array for 1D shape', () => {
    expect(reshape([1, 2, 3, 4], [4])).toEqual([1, 2, 3, 4]);
  });

  test('should reshape to 2D array', () => {
    expect(reshape([1, 2, 3, 4, 5, 6], [2, 3])).toEqual([
      [1, 2, 3],
      [4, 5, 6]
    ]);
  });

  test('should reshape to 3D array', () => {
    expect(reshape([1, 2, 3, 4, 5, 6, 7, 8], [2, 2, 2])).toEqual([
      [[1, 2], [3, 4]],
      [[5, 6], [7, 8]]
    ]);
  });

  test('should handle single element', () => {
    expect(reshape([42], [1, 1])).toEqual([[42]]);
  });
});

describe('getAtIndex', () => {
  test('should get element from 1D array', () => {
    expect(getAtIndex([1, 2, 3, 4], [2])).toBe(3);
  });

  test('should get element from 2D array', () => {
    const data = [[1, 2, 3], [4, 5, 6]];
    expect(getAtIndex(data, [1, 2])).toBe(6);
  });

  test('should get element from 3D array', () => {
    const data = [
      [[1, 2], [3, 4]],
      [[5, 6], [7, 8]]
    ];
    expect(getAtIndex(data, [1, 0, 1])).toBe(6);
  });

  test('should throw error for invalid index', () => {
    expect(() => getAtIndex([1, 2, 3], [0, 0])).toThrow('Index out of bounds');
  });

  test('should get first element', () => {
    expect(getAtIndex([[1, 2], [3, 4]], [0, 0])).toBe(1);
  });
});

describe('setAtIndex', () => {
  test('should set element in 1D array', () => {
    const data = [1, 2, 3, 4];
    setAtIndex(data, [2], 99);
    expect(data).toEqual([1, 2, 99, 4]);
  });

  test('should set element in 2D array', () => {
    const data = [[1, 2, 3], [4, 5, 6]];
    setAtIndex(data, [1, 2], 99);
    expect(data).toEqual([[1, 2, 3], [4, 5, 99]]);
  });

  test('should set element in 3D array', () => {
    const data = [
      [[1, 2], [3, 4]],
      [[5, 6], [7, 8]]
    ];
    setAtIndex(data, [1, 0, 1], 99);
    expect(data).toEqual([
      [[1, 2], [3, 4]],
      [[5, 99], [7, 8]]
    ]);
  });

  test('should throw error for invalid index in nested structure', () => {
    expect(() => setAtIndex([1, 2, 3], [0, 0], 99)).toThrow('Index out of bounds');
  });

  test('should throw error for invalid index beyond array', () => {
    const data = [[1, 2], [3, 4]];
    expect(() => setAtIndex(data, [0, 0, 0], 99)).toThrow('Index out of bounds');
  });

  test('should throw error when intermediate value is not an array', () => {
    const data: any = [1, 2, 3];
    // Need 3+ indices: [0] gets 1 (scalar), then tries to navigate through it with [1]
    // This triggers the error at line 99 during the loop
    expect(() => setAtIndex(data, [0, 1, 2], 99)).toThrow('Index out of bounds');
  });
});

describe('deepClone', () => {
  test('should clone primitive values', () => {
    expect(deepClone(5)).toBe(5);
    expect(deepClone('hello')).toBe('hello');
    expect(deepClone(true)).toBe(true);
    expect(deepClone(null)).toBe(null);
  });

  test('should clone arrays', () => {
    const original = [1, 2, 3];
    const cloned = deepClone(original);
    expect(cloned).toEqual(original);
    expect(cloned).not.toBe(original);

    cloned[0] = 99;
    expect(original[0]).toBe(1);
  });

  test('should clone nested arrays', () => {
    const original = [[1, 2], [3, 4]];
    const cloned = deepClone(original);
    expect(cloned).toEqual(original);
    expect(cloned).not.toBe(original);
    expect(cloned[0]).not.toBe(original[0]);

    cloned[0][0] = 99;
    expect(original[0][0]).toBe(1);
  });

  test('should clone objects', () => {
    const original = { a: 1, b: 2, c: { d: 3 } };
    const cloned = deepClone(original);
    expect(cloned).toEqual(original);
    expect(cloned).not.toBe(original);
    expect(cloned.c).not.toBe(original.c);

    cloned.c.d = 99;
    expect(original.c.d).toBe(3);
  });

  test('should clone mixed structures', () => {
    const original = {
      arr: [1, 2, [3, 4]],
      obj: { x: 10, y: 20 },
      num: 42
    };
    const cloned = deepClone(original);
    expect(cloned).toEqual(original);
    expect(cloned).not.toBe(original);

    cloned.arr[2] = [99, 99];
    expect(original.arr[2]).toEqual([3, 4]);
  });
});

describe('arraysEqual', () => {
  test('should return true for identical 1D arrays', () => {
    expect(arraysEqual([1, 2, 3], [1, 2, 3])).toBe(true);
  });

  test('should return false for different 1D arrays', () => {
    expect(arraysEqual([1, 2, 3], [1, 2, 4])).toBe(false);
  });

  test('should return false for different length arrays', () => {
    expect(arraysEqual([1, 2, 3], [1, 2])).toBe(false);
  });

  test('should return true for identical 2D arrays', () => {
    expect(arraysEqual([[1, 2], [3, 4]], [[1, 2], [3, 4]])).toBe(true);
  });

  test('should return false for different 2D arrays', () => {
    expect(arraysEqual([[1, 2], [3, 4]], [[1, 2], [3, 5]])).toBe(false);
  });

  test('should return true for identical 3D arrays', () => {
    const a = [[[1, 2], [3, 4]], [[5, 6], [7, 8]]];
    const b = [[[1, 2], [3, 4]], [[5, 6], [7, 8]]];
    expect(arraysEqual(a, b)).toBe(true);
  });

  test('should return false for different 3D arrays', () => {
    const a = [[[1, 2], [3, 4]], [[5, 6], [7, 8]]];
    const b = [[[1, 2], [3, 4]], [[5, 6], [7, 9]]];
    expect(arraysEqual(a, b)).toBe(false);
  });

  test('should return true for empty arrays', () => {
    expect(arraysEqual([], [])).toBe(true);
  });

  test('should handle arrays with different types', () => {
    expect(arraysEqual([1, '2', 3], [1, '2', 3])).toBe(true);
    expect(arraysEqual([1, '2', 3], [1, 2, 3])).toBe(false);
  });

  test('should return false when nested arrays differ in structure', () => {
    expect(arraysEqual([[1, 2], 3], [[1, 2], [3]])).toBe(false);
  });
});
