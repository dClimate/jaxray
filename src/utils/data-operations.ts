/**
 * Data operation utilities for multi-dimensional arrays
 * Handles array traversal, selection, slicing, and mathematical operations
 */

import { NDArray, DataValue, FlatData, FlatDataStorage } from '../types.js';
import { deepClone } from '../utils.js';

/**
 * Coerce a leaf value into the number used by numeric reductions (sum/count/mean).
 * Numbers pass through (NaN excluded); booleans reduce as 1/0 to match the
 * element-wise `sum` reducer (`acc + (val as number)`). Everything else
 * (strings, null, undefined) is skipped by returning `undefined`.
 */
function toReducibleNumber(value: unknown): number | undefined {
  if (typeof value === 'number') {
    return Number.isNaN(value) ? undefined : value;
  }
  if (typeof value === 'boolean') {
    return value ? 1 : 0;
  }
  return undefined;
}

/**
 * Sum all values in N-dimensional array without flattening - O(n) time, O(1) extra space
 */
export function sumAll(data: NDArray): number {
  let sum = 0;
  const stack: any[] = [data];

  while (stack.length > 0) {
    const current = stack.pop()!;
    if (Array.isArray(current)) {
      for (let i = current.length - 1; i >= 0; i--) {
        stack.push(current[i]);
      }
    } else {
      const numeric = toReducibleNumber(current);
      if (numeric !== undefined) {
        sum += numeric;
      }
    }
  }

  return sum;
}

/**
 * Count all valid numeric elements in N-dimensional array without flattening - O(n) time, O(1) extra space
 */
export function countAll(data: NDArray): number {
  let count = 0;
  const stack: any[] = [data];

  while (stack.length > 0) {
    const current = stack.pop()!;
    if (Array.isArray(current)) {
      for (let i = current.length - 1; i >= 0; i--) {
        stack.push(current[i]);
      }
    } else if (toReducibleNumber(current) !== undefined) {
      count++;
    }
  }

  return count;
}

/** Sum numeric values directly from row-major storage. */
export function sumFlat(data: FlatDataStorage): number {
  let sum = 0;
  for (let index = 0; index < data.length; index++) {
    const numeric = toReducibleNumber(data[index]);
    if (numeric !== undefined) sum += numeric;
  }
  return sum;
}

/** Count valid numeric values directly from row-major storage. */
export function countFlat(data: FlatDataStorage): number {
  let count = 0;
  for (let index = 0; index < data.length; index++) {
    if (toReducibleNumber(data[index]) !== undefined) count++;
  }
  return count;
}

/**
 * Reduce one dimension of row-major storage without constructing the source's
 * nested representation. Reduction results remain flat until a consumer asks
 * the resulting DataArray for `.values`.
 */
export function reduceFlatAlongDimension(
  source: FlatData,
  dimIndex: number,
  operation: 'sum' | 'mean'
): FlatData {
  const outputShape = source.shape.filter((_, index) => index !== dimIndex);
  const outputSize = outputShape.reduce((size, dimension) => size * dimension, 1);
  const output = new Array<DataValue>(outputSize);
  const sourceStrides = new Array(source.shape.length);
  let stride = 1;
  for (let dim = source.shape.length - 1; dim >= 0; dim--) {
    sourceStrides[dim] = stride;
    stride *= source.shape[dim];
  }

  for (let outputOffset = 0; outputOffset < outputSize; outputOffset++) {
    let remainder = outputOffset;
    let sourceBaseOffset = 0;
    let outputDim = outputShape.length - 1;
    for (let dim = source.shape.length - 1; dim >= 0; dim--) {
      if (dim === dimIndex) continue;
      const index = remainder % outputShape[outputDim];
      remainder = Math.floor(remainder / outputShape[outputDim]);
      sourceBaseOffset += index * sourceStrides[dim];
      outputDim--;
    }

    const reducedDimensionIsEmpty = source.shape[dimIndex] === 0;
    const startAtFirstValue = operation === 'sum' &&
      dimIndex === 0 &&
      source.shape.length > 1 &&
      !reducedDimensionIsEmpty;
    let sum: any = startAtFirstValue
      ? source.data[sourceBaseOffset]
      : 0;
    let count = 0;
    for (let index = startAtFirstValue ? 1 : 0; index < source.shape[dimIndex]; index++) {
      const value = source.data[sourceBaseOffset + index * sourceStrides[dimIndex]];
      if (operation === 'sum') {
        sum = sum + (value as any);
      } else {
        const numeric = toReducibleNumber(value);
        if (numeric !== undefined) {
          sum += numeric;
          count++;
        }
      }
    }
    output[outputOffset] = operation === 'mean'
      ? (count === 0 ? Number.NaN : sum / count)
      : sum;
  }

  return { data: output, shape: outputShape };
}

/**
 * Compute the mean along a dimension, skipping non-numeric and NaN values.
 */
export function meanAlongDimension(data: NDArray, dimIndex: number): NDArray {
  if (!Array.isArray(data)) {
    return Number.NaN;
  }

  if (dimIndex > 0) {
    return data.map(item => meanAlongDimension(item, dimIndex - 1)) as NDArray;
  }

  const meanAcrossFirstDimension = (values: any[]): any => {
    if (values.length === 0) {
      return Number.NaN;
    }

    if (Array.isArray(values[0])) {
      return values[0].map((_: any, index: number) =>
        meanAcrossFirstDimension(values.map(value => value[index]))
      );
    }

    let sum = 0;
    let count = 0;
    for (const value of values) {
      const numeric = toReducibleNumber(value);
      if (numeric !== undefined) {
        sum += numeric;
        count++;
      }
    }

    return count === 0 ? Number.NaN : sum / count;
  };

  return meanAcrossFirstDimension(data);
}

/**
 * Divide all values in an N-dimensional array by a scalar
 */
export function divideArray(data: NDArray, divisor: number): NDArray {
  if (!Array.isArray(data)) {
    return (data as number) / divisor;
  }

  return data.map((item: any) => divideArray(item, divisor)) as NDArray;
}

/**
 * Apply element-wise operation between two arrays of matching shapes
 */
export function elementWiseOp(a: any, b: any, op: (x: number, y: number) => number): any {
  if (!Array.isArray(a) && !Array.isArray(b)) {
    return op(a as number, b as number);
  }
  if (Array.isArray(a) && Array.isArray(b)) {
    return a.map((val, i) => elementWiseOp(val, b[i], op));
  }
  throw new Error('Mismatched array dimensions');
}

/**
 * Reshape array by removing dimensions of size 1
 */
export function reshapeSqueezed(data: NDArray, squeezedDims: number[]): NDArray {
  if (!Array.isArray(data) || squeezedDims.length === 0) {
    return data;
  }

  const helper = (input: any, dimIndex: number): any => {
    if (!Array.isArray(input)) {
      return input;
    }

    if (squeezedDims.includes(dimIndex) && input.length === 1) {
      return helper(input[0], dimIndex + 1);
    }

    return input.map(child => helper(child, dimIndex + 1));
  };

  return helper(data, 0) as NDArray;
}

/**
 * Select a single index along a specific dimension
 * Returns a lower-dimensional array (dimension is dropped)
 */
export function selectAtDimension(data: any, dimIndex: number, index: number): any {
  if (dimIndex === 0) {
    return data[index];
  }

  if (!Array.isArray(data)) {
    throw new Error('Invalid dimension index');
  }

  return data.map((item: any) => selectAtDimension(item, dimIndex - 1, index));
}

/**
 * Select multiple indices along a specific dimension
 * Returns an array with the same number of dimensions
 */
export function selectMultipleAtDimension(data: any, dimIndex: number, indices: number[]): any {
  if (dimIndex === 0) {
    return indices.map(i => data[i]);
  }

  if (!Array.isArray(data)) {
    throw new Error('Invalid dimension index');
  }

  return data.map((item: any) => selectMultipleAtDimension(item, dimIndex - 1, indices));
}

/**
 * Slice a range along a specific dimension
 */
export function sliceAtDimension(data: any, dimIndex: number, start: number, stop: number): any {
  if (dimIndex === 0) {
    return data.slice(start, stop);
  }

  if (!Array.isArray(data)) {
    throw new Error('Invalid dimension index');
  }

  return data.map((item: any) => sliceAtDimension(item, dimIndex - 1, start, stop));
}

/**
 * Reduce along a dimension using a reducer function
 * Used for operations like sum, mean, max, min, etc.
 */
export function reduceAlongDimension(
  data: NDArray,
  dimIndex: number,
  dims: string[],
  coords: { [key: string]: any[] },
  reducer: (acc: number, val: number) => number
): any {
  if (dimIndex === 0) {
    // Reducing the first dimension
    const dataArray = data as any[];
    if (!Array.isArray(dataArray) || dataArray.length === 0) {
      return dataArray;
    }

    // Check if elements are arrays (multi-dimensional)
    if (Array.isArray(dataArray[0])) {
      // Element-wise reduction across first dimension
      return dataArray.reduce((acc: any, row: any) => {
        if (!acc) return deepClone(row);
        if (Array.isArray(row)) {
          return elementWiseOp(acc, row, reducer);
        }
        return reducer(acc as number, row as number);
      });
    } else {
      // Simple 1D reduction
      return dataArray.reduce((acc: number, val: any) => reducer(acc, val as number), 0);
    }
  } else {
    // Reducing a later dimension - recurse into structure
    const dataArray = data as any[];
    return dataArray.map((item: any) => {
      // Create a pseudo-subarray structure for recursion
      // This is a simplified version - the actual DataArray class handles this better
      return reduceAlongDimension(
        item,
        dimIndex - 1,
        dims.slice(1),
        Object.fromEntries(
          Object.entries(coords).filter(([k]) => dims.slice(1).includes(k))
        ),
        reducer
      );
    });
  }
}
