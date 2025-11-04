/**
 * Tests for data-operations utility functions
 * Tests multi-dimensional array operations including traversal, selection, slicing, and math operations
 */

import { describe, it, expect } from 'vitest';
import {
  sumAll,
  countAll,
  divideArray,
  elementWiseOp,
  reshapeSqueezed,
  selectAtDimension,
  selectMultipleAtDimension,
  sliceAtDimension,
  reduceAlongDimension,
} from '../../src/utils/data-operations.js';

describe('sumAll', () => {
  it('should sum all values in a 1D array', () => {
    const data = [1, 2, 3, 4, 5];
    expect(sumAll(data)).toBe(15);
  });

  it('should sum all values in a 2D array', () => {
    const data = [[1, 2], [3, 4], [5, 6]];
    expect(sumAll(data)).toBe(21);
  });

  it('should sum all values in a 3D array', () => {
    const data = [[[1, 2], [3, 4]], [[5, 6], [7, 8]]];
    expect(sumAll(data)).toBe(36);
  });

  it('should handle single number', () => {
    expect(sumAll(5)).toBe(5);
  });

  it('should handle empty array', () => {
    expect(sumAll([])).toBe(0);
  });

  it('should handle nested empty arrays', () => {
    const data = [[], [[]], [[], []]];
    expect(sumAll(data)).toBe(0);
  });

  it('should handle arrays with zeros', () => {
    const data = [0, 1, 0, 2, 0, 3];
    expect(sumAll(data)).toBe(6);
  });

  it('should handle negative numbers', () => {
    const data = [-1, -2, 3, 4];
    expect(sumAll(data)).toBe(4);
  });
});

describe('countAll', () => {
  it('should count all elements in a 1D array', () => {
    const data = [1, 2, 3, 4, 5];
    expect(countAll(data)).toBe(5);
  });

  it('should count all elements in a 2D array', () => {
    const data = [[1, 2], [3, 4], [5, 6]];
    expect(countAll(data)).toBe(6);
  });

  it('should count all elements in a 3D array', () => {
    const data = [[[1, 2], [3, 4]], [[5, 6], [7, 8]]];
    expect(countAll(data)).toBe(8);
  });

  it('should count single number', () => {
    expect(countAll(5)).toBe(1);
  });

  it('should handle empty array', () => {
    expect(countAll([])).toBe(0);
  });

  it('should handle nested empty arrays', () => {
    const data = [[], [[]], [[], []]];
    expect(countAll(data)).toBe(0);
  });

  it('should count arrays with mixed dimensions', () => {
    const data = [[1, 2, 3], [4, 5]];
    expect(countAll(data)).toBe(5);
  });
});

describe('divideArray', () => {
  it('should divide all values in a 1D array', () => {
    const data = [2, 4, 6, 8];
    const result = divideArray(data, 2);
    expect(result).toEqual([1, 2, 3, 4]);
  });

  it('should divide all values in a 2D array', () => {
    const data = [[10, 20], [30, 40]];
    const result = divideArray(data, 10);
    expect(result).toEqual([[1, 2], [3, 4]]);
  });

  it('should divide all values in a 3D array', () => {
    const data = [[[2, 4], [6, 8]], [[10, 12], [14, 16]]];
    const result = divideArray(data, 2);
    expect(result).toEqual([[[1, 2], [3, 4]], [[5, 6], [7, 8]]]);
  });

  it('should handle single number', () => {
    expect(divideArray(10, 2)).toBe(5);
  });

  it('should handle division by 1', () => {
    const data = [1, 2, 3];
    const result = divideArray(data, 1);
    expect(result).toEqual([1, 2, 3]);
  });

  it('should handle fractional results', () => {
    const data = [1, 2, 3];
    const result = divideArray(data, 2);
    expect(result).toEqual([0.5, 1, 1.5]);
  });

  it('should not mutate original array', () => {
    const data = [2, 4, 6];
    const original = [...data];
    divideArray(data, 2);
    expect(data).toEqual(original);
  });
});

describe('elementWiseOp', () => {
  it('should apply operation on two numbers', () => {
    const result = elementWiseOp(5, 3, (a, b) => a + b);
    expect(result).toBe(8);
  });

  it('should apply element-wise addition on 1D arrays', () => {
    const a = [1, 2, 3];
    const b = [4, 5, 6];
    const result = elementWiseOp(a, b, (x, y) => x + y);
    expect(result).toEqual([5, 7, 9]);
  });

  it('should apply element-wise subtraction on 1D arrays', () => {
    const a = [10, 20, 30];
    const b = [1, 2, 3];
    const result = elementWiseOp(a, b, (x, y) => x - y);
    expect(result).toEqual([9, 18, 27]);
  });

  it('should apply element-wise multiplication on 1D arrays', () => {
    const a = [2, 3, 4];
    const b = [5, 6, 7];
    const result = elementWiseOp(a, b, (x, y) => x * y);
    expect(result).toEqual([10, 18, 28]);
  });

  it('should apply element-wise operation on 2D arrays', () => {
    const a = [[1, 2], [3, 4]];
    const b = [[5, 6], [7, 8]];
    const result = elementWiseOp(a, b, (x, y) => x + y);
    expect(result).toEqual([[6, 8], [10, 12]]);
  });

  it('should apply element-wise operation on 3D arrays', () => {
    const a = [[[1, 2]], [[3, 4]]];
    const b = [[[10, 20]], [[30, 40]]];
    const result = elementWiseOp(a, b, (x, y) => x + y);
    expect(result).toEqual([[[11, 22]], [[33, 44]]]);
  });

  it('should throw error on mismatched dimensions (array vs scalar)', () => {
    expect(() => {
      elementWiseOp([1, 2, 3], 5, (x, y) => x + y);
    }).toThrow('Mismatched array dimensions');
  });

  it('should throw error on mismatched dimensions (scalar vs array)', () => {
    expect(() => {
      elementWiseOp(5, [1, 2, 3], (x, y) => x + y);
    }).toThrow('Mismatched array dimensions');
  });

  it('should handle custom operations', () => {
    const a = [2, 4, 6];
    const b = [1, 2, 3];
    const result = elementWiseOp(a, b, (x, y) => Math.max(x, y));
    expect(result).toEqual([2, 4, 6]);
  });
});

describe('reshapeSqueezed', () => {
  it('should squeeze dimension 0 if size is 1', () => {
    const data = [[1, 2, 3]];
    const result = reshapeSqueezed(data, [0]);
    expect(result).toEqual([1, 2, 3]);
  });

  it('should squeeze dimension 1 if size is 1', () => {
    const data = [[1], [2], [3]];
    const result = reshapeSqueezed(data, [1]);
    expect(result).toEqual([1, 2, 3]);
  });

  it('should squeeze multiple dimensions', () => {
    const data = [[[1, 2, 3]]];
    const result = reshapeSqueezed(data, [0, 1]);
    expect(result).toEqual([1, 2, 3]);
  });

  it('should not squeeze if dimension size is not 1', () => {
    const data = [[1, 2], [3, 4]];
    const result = reshapeSqueezed(data, [0]);
    expect(result).toEqual([[1, 2], [3, 4]]);
  });

  it('should handle empty squeezed dimensions array', () => {
    const data = [[1, 2], [3, 4]];
    const result = reshapeSqueezed(data, []);
    expect(result).toEqual([[1, 2], [3, 4]]);
  });

  it('should handle non-array data', () => {
    expect(reshapeSqueezed(5, [0, 1])).toBe(5);
  });

  it('should handle complex nested structure', () => {
    const data = [[[[1], [2]], [[3], [4]]]];
    const result = reshapeSqueezed(data, [0, 2]);
    // Squeezing dimension 0 removes outer wrapper: [[[1], [2]], [[3], [4]]]
    // Squeezing dimension 2 removes innermost single-element arrays: [[[1], [2]], [[3], [4]]]
    // But dimension 2 doesn't have size 1 at all levels, so it stays mostly the same
    expect(result).toEqual([[[1], [2]], [[3], [4]]]);
  });

  it('should not mutate original array', () => {
    const data = [[[1, 2, 3]]];
    const original = JSON.stringify(data);
    reshapeSqueezed(data, [0]);
    expect(JSON.stringify(data)).toBe(original);
  });
});

describe('selectAtDimension', () => {
  it('should select index at dimension 0 from 1D array', () => {
    const data = [10, 20, 30, 40];
    expect(selectAtDimension(data, 0, 2)).toBe(30);
  });

  it('should select index at dimension 0 from 2D array', () => {
    const data = [[1, 2], [3, 4], [5, 6]];
    const result = selectAtDimension(data, 0, 1);
    expect(result).toEqual([3, 4]);
  });

  it('should select index at dimension 1 from 2D array', () => {
    const data = [[1, 2, 3], [4, 5, 6], [7, 8, 9]];
    const result = selectAtDimension(data, 1, 1);
    expect(result).toEqual([2, 5, 8]);
  });

  it('should select index at dimension 0 from 3D array', () => {
    const data = [[[1, 2]], [[3, 4]], [[5, 6]]];
    const result = selectAtDimension(data, 0, 1);
    expect(result).toEqual([[3, 4]]);
  });

  it('should select index at dimension 2 from 3D array', () => {
    const data = [[[1, 2, 3], [4, 5, 6]], [[7, 8, 9], [10, 11, 12]]];
    const result = selectAtDimension(data, 2, 1);
    expect(result).toEqual([[2, 5], [8, 11]]);
  });

  it('should handle first index', () => {
    const data = [10, 20, 30];
    expect(selectAtDimension(data, 0, 0)).toBe(10);
  });

  it('should handle last index', () => {
    const data = [10, 20, 30];
    expect(selectAtDimension(data, 0, 2)).toBe(30);
  });

  it('should throw error for invalid dimension index', () => {
    const data = [1, 2, 3];
    expect(() => {
      selectAtDimension(data, 5, 0);
    }).toThrow('Invalid dimension index');
  });
});

describe('selectMultipleAtDimension', () => {
  it('should select multiple indices at dimension 0 from 1D array', () => {
    const data = [10, 20, 30, 40, 50];
    const result = selectMultipleAtDimension(data, 0, [1, 3]);
    expect(result).toEqual([20, 40]);
  });

  it('should select multiple indices at dimension 0 from 2D array', () => {
    const data = [[1, 2], [3, 4], [5, 6], [7, 8]];
    const result = selectMultipleAtDimension(data, 0, [0, 2]);
    expect(result).toEqual([[1, 2], [5, 6]]);
  });

  it('should select multiple indices at dimension 1 from 2D array', () => {
    const data = [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]];
    const result = selectMultipleAtDimension(data, 1, [0, 2]);
    expect(result).toEqual([[1, 3], [5, 7], [9, 11]]);
  });

  it('should select multiple indices at dimension 2 from 3D array', () => {
    const data = [[[1, 2, 3], [4, 5, 6]], [[7, 8, 9], [10, 11, 12]]];
    const result = selectMultipleAtDimension(data, 2, [0, 2]);
    expect(result).toEqual([[[1, 3], [4, 6]], [[7, 9], [10, 12]]]);
  });

  it('should handle single index selection', () => {
    const data = [10, 20, 30, 40];
    const result = selectMultipleAtDimension(data, 0, [2]);
    expect(result).toEqual([30]);
  });

  it('should handle empty indices array', () => {
    const data = [10, 20, 30, 40];
    const result = selectMultipleAtDimension(data, 0, []);
    expect(result).toEqual([]);
  });

  it('should handle non-sequential indices', () => {
    const data = [10, 20, 30, 40, 50];
    const result = selectMultipleAtDimension(data, 0, [4, 1, 3]);
    expect(result).toEqual([50, 20, 40]);
  });

  it('should throw error for invalid dimension index', () => {
    const data = [1, 2, 3];
    expect(() => {
      selectMultipleAtDimension(data, 5, [0, 1]);
    }).toThrow('Invalid dimension index');
  });
});

describe('sliceAtDimension', () => {
  it('should slice at dimension 0 from 1D array', () => {
    const data = [10, 20, 30, 40, 50];
    const result = sliceAtDimension(data, 0, 1, 4);
    expect(result).toEqual([20, 30, 40]);
  });

  it('should slice at dimension 0 from 2D array', () => {
    const data = [[1, 2], [3, 4], [5, 6], [7, 8]];
    const result = sliceAtDimension(data, 0, 1, 3);
    expect(result).toEqual([[3, 4], [5, 6]]);
  });

  it('should slice at dimension 1 from 2D array', () => {
    const data = [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]];
    const result = sliceAtDimension(data, 1, 1, 3);
    expect(result).toEqual([[2, 3], [6, 7], [10, 11]]);
  });

  it('should slice at dimension 2 from 3D array', () => {
    const data = [[[1, 2, 3, 4]], [[5, 6, 7, 8]]];
    const result = sliceAtDimension(data, 2, 1, 3);
    expect(result).toEqual([[[2, 3]], [[6, 7]]]);
  });

  it('should handle slice from beginning', () => {
    const data = [10, 20, 30, 40];
    const result = sliceAtDimension(data, 0, 0, 2);
    expect(result).toEqual([10, 20]);
  });

  it('should handle slice to end', () => {
    const data = [10, 20, 30, 40];
    const result = sliceAtDimension(data, 0, 2, 4);
    expect(result).toEqual([30, 40]);
  });

  it('should handle empty slice', () => {
    const data = [10, 20, 30, 40];
    const result = sliceAtDimension(data, 0, 2, 2);
    expect(result).toEqual([]);
  });

  it('should handle full slice', () => {
    const data = [10, 20, 30];
    const result = sliceAtDimension(data, 0, 0, 3);
    expect(result).toEqual([10, 20, 30]);
  });

  it('should throw error for invalid dimension index', () => {
    const data = [1, 2, 3];
    expect(() => {
      sliceAtDimension(data, 5, 0, 2);
    }).toThrow('Invalid dimension index');
  });

  it('should not mutate original array', () => {
    const data = [10, 20, 30, 40];
    const original = [...data];
    sliceAtDimension(data, 0, 1, 3);
    expect(data).toEqual(original);
  });
});

describe('reduceAlongDimension', () => {
  const sumReducer = (acc: number, val: number) => acc + val;
  const maxReducer = (acc: number, val: number) => Math.max(acc, val);

  it('should reduce 1D array along dimension 0', () => {
    const data = [1, 2, 3, 4, 5];
    const result = reduceAlongDimension(data, 0, ['x'], { x: [] }, sumReducer);
    expect(result).toBe(15);
  });

  it('should reduce 2D array along dimension 0 (sum rows)', () => {
    const data = [[1, 2, 3], [4, 5, 6], [7, 8, 9]];
    const result = reduceAlongDimension(data, 0, ['x', 'y'], { x: [], y: [] }, sumReducer);
    expect(result).toEqual([12, 15, 18]);
  });

  it('should reduce 2D array along dimension 1 (sum columns)', () => {
    const data = [[1, 2, 3], [4, 5, 6]];
    const result = reduceAlongDimension(data, 1, ['x', 'y'], { x: [], y: [] }, sumReducer);
    expect(result).toEqual([6, 15]);
  });

  it('should reduce 3D array along dimension 0', () => {
    const data = [[[1, 2]], [[3, 4]], [[5, 6]]];
    const result = reduceAlongDimension(data, 0, ['x', 'y', 'z'], { x: [], y: [], z: [] }, sumReducer);
    expect(result).toEqual([[9, 12]]);
  });

  it('should handle max reduction', () => {
    const data = [[1, 5, 3], [4, 2, 6], [7, 8, 0]];
    const result = reduceAlongDimension(data, 0, ['x', 'y'], { x: [], y: [] }, maxReducer);
    expect(result).toEqual([7, 8, 6]);
  });

  it('should handle min reduction', () => {
    const data = [[5, 2, 8], [3, 9, 1], [4, 6, 7]];
    const minReducer = (acc: number, val: number) => Math.min(acc, val);
    const result = reduceAlongDimension(data, 0, ['x', 'y'], { x: [], y: [] }, minReducer);
    expect(result).toEqual([3, 2, 1]);
  });

  it('should handle empty array', () => {
    const data: number[] = [];
    const result = reduceAlongDimension(data, 0, ['x'], { x: [] }, sumReducer);
    expect(result).toEqual([]);
  });

  it('should handle single element array', () => {
    const data = [42];
    const result = reduceAlongDimension(data, 0, ['x'], { x: [] }, sumReducer);
    expect(result).toBe(42);
  });

  it('should handle 2D array with single row', () => {
    const data = [[1, 2, 3]];
    const result = reduceAlongDimension(data, 0, ['x', 'y'], { x: [], y: [] }, sumReducer);
    expect(result).toEqual([1, 2, 3]);
  });

  it('should reduce complex 3D array along dimension 1', () => {
    const data = [[[1, 2], [3, 4]], [[5, 6], [7, 8]]];
    const result = reduceAlongDimension(data, 1, ['x', 'y', 'z'], { x: [], y: [], z: [] }, sumReducer);
    expect(result).toEqual([[4, 6], [12, 14]]);
  });

  it('should handle multiplication reduction', () => {
    const data = [[2, 3], [4, 5]];
    const multiplyReducer = (acc: number, val: number) => acc * val;
    const result = reduceAlongDimension(data, 0, ['x', 'y'], { x: [], y: [] }, multiplyReducer);
    // Reduces along dimension 0: [2,3] * [4,5] = [2*4, 3*5] = [8, 15]
    expect(result).toEqual([8, 15]);
  });
});
