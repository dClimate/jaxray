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

/**
 * Reduce every leaf of an N-dimensional array to a scalar, skipping masked and
 * non-numeric leaves. Iterative (an explicit stack, like `sumAll`) so a deeply
 * nested array can't blow the call stack. Returns NaN when nothing is numeric.
 *
 * `median` is the one operation that must retain the values it visits; the
 * others accumulate in O(1) space.
 */
export function reduceAll(data: NDArray, operation: ReduceOperation): number {
  const collected: number[] | null = operation === 'median' ? [] : null;
  const std = operation === 'std' ? welfordStd() : null;
  let sum = 0;
  let count = 0;
  let extreme = Number.NaN;
  let seen = false;

  // One frame per level of nesting, each holding a cursor into its array, so the
  // stack stays O(depth). Pushing every child instead would hold a reference to
  // each element at once — O(n) for the flat 1-D case that dominates in practice.
  const frames: Array<{ array: any[]; index: number }> = [
    { array: Array.isArray(data) ? data : [data], index: 0 }
  ];

  while (frames.length > 0) {
    const frame = frames[frames.length - 1];
    if (frame.index >= frame.array.length) {
      frames.pop();
      continue;
    }

    const current = frame.array[frame.index++];
    if (Array.isArray(current)) {
      frames.push({ array: current, index: 0 });
      continue;
    }

    const numeric = toReducibleNumber(current);
    if (numeric === undefined) continue;

    if (collected) {
      collected.push(numeric);
    } else if (std) {
      std.push(numeric);
    } else if (!seen) {
      extreme = numeric;
    } else if (operation === 'min') {
      if (numeric < extreme) extreme = numeric;
    } else if (operation === 'max') {
      if (numeric > extreme) extreme = numeric;
    }

    sum += numeric;
    count++;
    seen = true;
  }

  switch (operation) {
    case 'sum':
      return sum;
    case 'mean':
      return count === 0 ? Number.NaN : sum / count;
    case 'median':
      return medianOf(collected ?? []);
    case 'std':
      return std ? std.result() : Number.NaN;
    default:
      return seen ? extreme : Number.NaN;
  }
}

/** Reduce row-major storage to a scalar, skipping masked and non-numeric values. */
export function reduceFlat(data: FlatDataStorage, operation: ReduceOperation): number {
  const collected: number[] | null = operation === 'median' ? [] : null;
  const std = operation === 'std' ? welfordStd() : null;
  let sum = 0;
  let count = 0;
  let extreme = Number.NaN;
  let seen = false;

  for (let index = 0; index < data.length; index++) {
    const numeric = toReducibleNumber(data[index]);
    if (numeric === undefined) continue;

    if (collected) {
      collected.push(numeric);
    } else if (std) {
      std.push(numeric);
    } else if (!seen) {
      extreme = numeric;
    } else if (operation === 'min') {
      if (numeric < extreme) extreme = numeric;
    } else if (operation === 'max') {
      if (numeric > extreme) extreme = numeric;
    }

    sum += numeric;
    count++;
    seen = true;
  }

  switch (operation) {
    case 'sum':
      return sum;
    case 'mean':
      return count === 0 ? Number.NaN : sum / count;
    case 'median':
      return medianOf(collected ?? []);
    case 'std':
      return std ? std.result() : Number.NaN;
    default:
      return seen ? extreme : Number.NaN;
  }
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
 * Reductions that skip masked/non-numeric leaves and return NaN for an
 * all-masked slice, matching xarray's skipna default.
 */
export type ReduceOperation = 'sum' | 'mean' | 'min' | 'max' | 'std' | 'median';

/**
 * Median of the numeric values in `values`, or NaN when none are numeric.
 * Sorts a copy: unlike the streaming reductions this needs every value at once,
 * so callers pay O(k log k) time and O(k) space in the reduced extent.
 */
function medianOf(values: number[]): number {
  if (values.length === 0) return Number.NaN;
  const sorted = values.slice().sort((a, b) => a - b);
  const mid = sorted.length >> 1;
  if (sorted.length % 2 !== 0) return sorted[mid];
  // Halve before adding: `(a + b) / 2` overflows to Infinity when the pair sums
  // past Number.MAX_VALUE, even though the median itself is finite.
  return sorted[mid - 1] / 2 + sorted[mid] / 2;
}

/**
 * Population standard deviation (ddof=0, as xarray defaults) via Welford's
 * algorithm — one pass, and numerically stable for the large means climate
 * fields carry (e.g. temperatures in Kelvin), where the textbook
 * `E[x²] - E[x]²` form loses most of its significant digits.
 */
function welfordStd(): {
  push: (value: number) => void;
  result: () => number;
} {
  let count = 0;
  let mean = 0;
  let m2 = 0;
  return {
    push(value: number) {
      count++;
      const delta = value - mean;
      mean += delta / count;
      m2 += delta * (value - mean);
    },
    result() {
      return count === 0 ? Number.NaN : Math.sqrt(m2 / count);
    }
  };
}

/**
 * Reduce one dimension of row-major storage without constructing the source's
 * nested representation. Reduction results remain flat until a consumer asks
 * the resulting DataArray for `.values`.
 */
export function reduceFlatAlongDimension(
  source: FlatData,
  dimIndex: number,
  operation: ReduceOperation
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

    // min/max/std/median share the stride walk but not the running-sum
    // accumulator, so they branch off before it.
    if (operation !== 'sum' && operation !== 'mean') {
      let extreme = Number.NaN;
      let seen = false;
      const std = operation === 'std' ? welfordStd() : null;
      const collected: number[] | null = operation === 'median' ? [] : null;

      for (let index = 0; index < source.shape[dimIndex]; index++) {
        const value = source.data[sourceBaseOffset + index * sourceStrides[dimIndex]];
        const numeric = toReducibleNumber(value);
        if (numeric === undefined) continue;

        if (collected) {
          collected.push(numeric);
        } else if (std) {
          std.push(numeric);
        } else if (!seen) {
          extreme = numeric;
        } else if (operation === 'min') {
          if (numeric < extreme) extreme = numeric;
        } else if (numeric > extreme) {
          extreme = numeric;
        }
        seen = true;
      }

      output[outputOffset] =
        collected ? medianOf(collected)
        : std ? std.result()
        : seen ? extreme
        : Number.NaN;
      continue;
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
/**
 * Reduce one dimension of a nested array in place, without flattening the whole
 * source first. Mirrors `meanAlongDimension` and exists for the same reason:
 * arrays built from nested data have no row-major storage, and copying a large
 * grid just to reduce it costs O(total elements) of extra memory.
 *
 * Skips masked and non-numeric leaves; an all-masked slice reduces to NaN.
 */
export function reduceOpAlongDimension(
  data: NDArray,
  dimIndex: number,
  operation: ReduceOperation
): NDArray {
  if (!Array.isArray(data)) {
    return Number.NaN as unknown as NDArray;
  }

  if (dimIndex > 0) {
    return data.map(item => reduceOpAlongDimension(item as NDArray, dimIndex - 1, operation)) as NDArray;
  }

  const reduceAcrossFirstDimension = (values: any[]): any => {
    if (values.length === 0) {
      return Number.NaN;
    }

    // Still nested: recurse position-wise so the reduction lands on the leaves.
    if (Array.isArray(values[0])) {
      return values[0].map((_: any, index: number) =>
        reduceAcrossFirstDimension(values.map(value => value[index]))
      );
    }

    return reduceAll(values as NDArray, operation);
  };

  return reduceAcrossFirstDimension(data as any[]);
}

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
