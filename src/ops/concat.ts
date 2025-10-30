/**
 * Concatenation operations for DataArrays and Datasets
 * Provides lazy concatenation along specified dimensions
 */

import {
  NDArray,
  DataValue,
  DimensionName,
  CoordinateValue,
  Coordinates,
  Attributes,
  LazyIndexRange
} from '../types.js';
import { deepClone } from '../utils.js';

export interface ConcatOptions {
  dim: DimensionName;
  fillValue?: DataValue;
}

export interface DataArrayConcatOperand {
  getData: (ranges: { [dimension: string]: LazyIndexRange }) => Promise<NDArray>;
  dims: DimensionName[];
  shape: number[];
  coords: Coordinates;
  attrs: Attributes;
  name?: string;
  isLazy: boolean;
}

export interface ConcatMetadata {
  dim: DimensionName;
  dimIndex: number;
  firstSize: number;
  secondSize: number;
  combinedSize: number;
  newShape: number[];
  newCoords: Coordinates;
  newAttrs: Attributes;
  name?: string;
}

/**
 * Validate that two DataArrays can be concatenated along the specified dimension
 */
export function validateConcatenation(
  first: DataArrayConcatOperand,
  second: DataArrayConcatOperand,
  dim: DimensionName
): void {
  // Validate dimensions match (except for the concat dimension)
  if (first.dims.length !== second.dims.length) {
    throw new Error(
      `Cannot concatenate DataArrays with different number of dimensions: ${first.dims.length} vs ${second.dims.length}`
    );
  }

  for (let i = 0; i < first.dims.length; i++) {
    const firstDim = first.dims[i];
    const secondDim = second.dims[i];

    if (firstDim !== secondDim) {
      throw new Error(
        `Dimension mismatch at position ${i}: '${firstDim}' vs '${secondDim}'`
      );
    }

    if (firstDim !== dim && first.shape[i] !== second.shape[i]) {
      throw new Error(
        `Shape mismatch for dimension '${firstDim}': ${first.shape[i]} vs ${second.shape[i]}`
      );
    }
  }

  // Validate dimension exists
  const dimIndex = first.dims.indexOf(dim);
  if (dimIndex === -1) {
    throw new Error(`Dimension '${dim}' not found in DataArray`);
  }
}

/**
 * Compute metadata for concatenation operation
 */
export function computeConcatMetadata(
  first: DataArrayConcatOperand,
  second: DataArrayConcatOperand,
  firstCoords: CoordinateValue[],
  secondCoords: CoordinateValue[],
  combinedCoords: CoordinateValue[],
  dim: DimensionName
): ConcatMetadata {
  const dimIndex = first.dims.indexOf(dim);

  // Calculate the new shape
  const newShape = [...first.shape];
  newShape[dimIndex] = combinedCoords.length;

  // Build new coordinates
  const newCoords: Coordinates = { ...first.coords };
  newCoords[dim] = combinedCoords;

  return {
    dim,
    dimIndex,
    firstSize: firstCoords.length,
    secondSize: secondCoords.length,
    combinedSize: combinedCoords.length,
    newShape,
    newCoords,
    newAttrs: deepClone(first.attrs),
    name: first.name
  };
}

/**
 * Create a lazy loader for concatenated DataArrays
 * Routes queries to the appropriate source DataArray based on the requested range
 */
export function createConcatLoader(
  first: DataArrayConcatOperand,
  second: DataArrayConcatOperand,
  metadata: ConcatMetadata
): (ranges: { [dimension: string]: LazyIndexRange }) => Promise<NDArray> {
  const { dim, dimIndex, firstSize } = metadata;
  const secondOffset = firstSize;

  return async (ranges: { [dimension: string]: LazyIndexRange }): Promise<NDArray> => {
    const requestedRange = ranges[dim];

    // Determine which indices are requested
    let startIdx: number;
    let stopIdx: number;

    if (typeof requestedRange === 'number') {
      startIdx = requestedRange;
      stopIdx = requestedRange + 1;
    } else if (requestedRange && typeof requestedRange === 'object') {
      startIdx = requestedRange.start;
      stopIdx = requestedRange.stop;
    } else {
      startIdx = 0;
      stopIdx = metadata.combinedSize;
    }

    // Determine which dataset(s) to query
    const firstEnd = firstSize;
    const secondStart = firstSize;

    const needsFirst = startIdx < firstEnd;
    const needsSecond = stopIdx > secondStart;

    if (needsFirst && !needsSecond) {
      // Only query first dataset
      const firstRanges = { ...ranges };
      if (typeof requestedRange === 'number') {
        firstRanges[dim] = requestedRange;
      } else {
        firstRanges[dim] = { start: startIdx, stop: Math.min(stopIdx, firstEnd) };
      }
      return await first.getData(firstRanges);
    } else if (needsSecond && !needsFirst) {
      // Only query second dataset
      const secondRanges = { ...ranges };
      const offsetStart = startIdx - secondOffset;
      const offsetStop = stopIdx - secondOffset;

      if (typeof requestedRange === 'number') {
        secondRanges[dim] = offsetStart;
      } else {
        secondRanges[dim] = { start: Math.max(0, offsetStart), stop: offsetStop };
      }
      return await second.getData(secondRanges);
    } else if (needsFirst && needsSecond) {
      // Query both datasets and concatenate results
      const firstRanges = { ...ranges };
      firstRanges[dim] = { start: startIdx, stop: firstEnd };
      const firstData = await first.getData(firstRanges);

      const secondRanges = { ...ranges };
      secondRanges[dim] = { start: 0, stop: stopIdx - secondOffset };
      const secondData = await second.getData(secondRanges);

      // Concatenate along the dimension
      return concatenateArrays(firstData, secondData, dimIndex);
    } else {
      // Empty selection
      throw new Error('Invalid range for concatenated dataset');
    }
  };
}

/**
 * Concatenate two NDArrays along a specific dimension
 */
export function concatenateArrays(arr1: NDArray, arr2: NDArray, dimIndex: number): NDArray {
  // Handle scalar case
  if (!Array.isArray(arr1)) {
    if (!Array.isArray(arr2)) {
      return [arr1, arr2] as NDArray;
    }
    throw new Error('Cannot concatenate scalar with array');
  }
  if (!Array.isArray(arr2)) {
    throw new Error('Cannot concatenate array with scalar');
  }

  // Concatenate along the specified dimension
  if (dimIndex === 0) {
    // Concatenate at the outermost level
    return ([...arr1, ...arr2] as NDArray);
  } else {
    // Recursively concatenate at deeper levels
    if (arr1.length !== arr2.length) {
      throw new Error(
        `Cannot concatenate arrays with different lengths at dimension 0: ${arr1.length} vs ${arr2.length}`
      );
    }

    const result: any[] = [];
    for (let i = 0; i < arr1.length; i++) {
      result.push(concatenateArrays(arr1[i] as NDArray, arr2[i] as NDArray, dimIndex - 1));
    }
    return result as NDArray;
  }
}
