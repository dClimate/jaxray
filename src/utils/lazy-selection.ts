/**
 * Lazy selection utilities for DataArray
 * Handles selection operations on lazy-loaded arrays without materializing the entire dataset
 */

import { Selection, DimensionName, Coordinates, SelectionOptions, LazyIndexRange, Attributes } from '../types.js';
import { deepClone } from '../utils.js';
import { findCoordinateIndex } from './coordinate-indexing.js';

/**
 * Map a current index to the original index in the source dataset.
 * If this DataArray has an originalIndexMapping (from a previous selection),
 * use it to look up the original index. Otherwise, the current index IS the original.
 */
export function mapIndexToOriginal(
  originalIndexMapping: { [dimension: string]: number[] } | undefined,
  dim: DimensionName,
  currentIndex: number
): number {
  if (originalIndexMapping && originalIndexMapping[dim]) {
    return originalIndexMapping[dim][currentIndex];
  }
  return currentIndex;
}

/**
 * Parameters for performing lazy selection
 */
export interface LazySelectionParams {
  selection: Selection;
  options?: SelectionOptions;
  dims: DimensionName[];
  shape: number[];
  coords: Coordinates;
  attrs: Attributes;
  name?: string;
  originalIndexMapping?: { [dimension: string]: number[] };
  lazyLoader: (ranges: Record<string, LazyIndexRange>) => Promise<any>;
}

/**
 * Result of lazy selection operation
 */
export interface LazySelectionResult {
  virtualShape: number[];
  lazyLoader: (ranges: Record<string, LazyIndexRange>) => Promise<any>;
  dims: DimensionName[];
  coords: Coordinates;
  attrs: Attributes;
  name?: string;
  originalIndexMapping: { [dimension: string]: number[] };
}

/**
 * Perform lazy selection on a DataArray
 * Returns metadata for creating a new lazy DataArray without materializing data
 */
export function performLazySelection(params: LazySelectionParams): LazySelectionResult {
  const {
    selection,
    options,
    dims,
    shape,
    coords,
    attrs,
    name,
    originalIndexMapping,
    lazyLoader: loader
  } = params;

  const indexRanges: Record<string, LazyIndexRange> = {};
  const fixedOriginalIndices: { [dim: string]: number } = {};
  const newDims: DimensionName[] = [];
  const newCoords: Coordinates = {};
  const newOriginalIndexMapping: { [dim: string]: number[] } = {};
  const parentIndexMapping: { [dim: string]: number[] } = {}; // Maps to parent virtual space

  // Get coordinate attributes for time conversion
  const coordAttrs = (attrs as any)?._coordAttrs;

  for (let i = 0; i < dims.length; i++) {
    const dim = dims[i];
    const sel = selection[dim];
    const dimAttrs = coordAttrs?.[dim] || attrs;
    if (sel === undefined) {
      const length = shape[i];
      // Parent mapping: identity (all indices 0 to length-1)
      const parentMapping = Array.from({ length }, (_, j) => j);
      // Original mapping: map through to original space
      const mapping = parentMapping.map(parentIdx =>
        mapIndexToOriginal(originalIndexMapping, dim, parentIdx)
      );

      if (parentMapping.length > 0) {
        indexRanges[dim] = {
          start: parentMapping[0],
          stop: parentMapping[parentMapping.length - 1] + 1
        };
        newOriginalIndexMapping[dim] = mapping;
        parentIndexMapping[dim] = parentMapping;
      } else {
        indexRanges[dim] = { start: 0, stop: 0 };
        newOriginalIndexMapping[dim] = [];
        parentIndexMapping[dim] = [];
      }

      newDims.push(dim);
      newCoords[dim] = coords[dim];
    } else if (
      typeof sel === 'number' ||
      typeof sel === 'string' ||
      typeof sel === 'bigint' ||
      sel instanceof Date
    ) {
      const index = findCoordinateIndex(coords[dim], sel, options, dim, dimAttrs);
      const originalIndex = mapIndexToOriginal(originalIndexMapping, dim, index);
      indexRanges[dim] = originalIndex;
      fixedOriginalIndices[dim] = originalIndex;
    } else if (Array.isArray(sel)) {
      const indices = sel.map(v =>
        findCoordinateIndex(coords[dim], v, options, dim, dimAttrs)
      );
      const minIdx = Math.min(...indices);
      const maxIdx = Math.max(...indices);

      // Parent mapping: indices in parent virtual space
      const parentMapping = Array.from(
        { length: maxIdx - minIdx + 1 },
        (_, j) => minIdx + j
      );

      // Original mapping: map through to original space
      const mapping = parentMapping.map(parentIdx =>
        mapIndexToOriginal(originalIndexMapping, dim, parentIdx)
      );

      indexRanges[dim] = {
        start: parentMapping[0],
        stop: parentMapping[parentMapping.length - 1] + 1
      };

      newDims.push(dim);
      newCoords[dim] = coords[dim].slice(minIdx, maxIdx + 1);
      newOriginalIndexMapping[dim] = mapping;
      parentIndexMapping[dim] = parentMapping;
    } else if (typeof sel === 'object' && ('start' in sel || 'stop' in sel)) {
      const { start, stop } = sel;
      const startIndex = start !== undefined ?
        findCoordinateIndex(coords[dim], start, options, dim, dimAttrs) : 0;
      const stopIndex = stop !== undefined ?
        findCoordinateIndex(coords[dim], stop, options, dim, dimAttrs) + 1 : shape[i];

      // Create mapping to parent's virtual space (for lazy loader)
      const parentMapping = Array.from(
        { length: Math.max(0, stopIndex - startIndex) },
        (_, j) => startIndex + j
      );

      // Create mapping to original space (for metadata and future selections)
      const mapping = parentMapping.map(parentIdx =>
        mapIndexToOriginal(originalIndexMapping, dim, parentIdx)
      );

      // Store parent mapping for lazy loader to use
      if (parentMapping.length > 0) {
        indexRanges[dim] = {
          start: parentMapping[0],
          stop: parentMapping[parentMapping.length - 1] + 1
        };
      } else {
        indexRanges[dim] = { start: 0, stop: 0 };
      }

      newDims.push(dim);
      newCoords[dim] = coords[dim].slice(startIndex, stopIndex);
      // Store original mapping for metadata and future selections
      newOriginalIndexMapping[dim] = mapping;
      // Store parent mapping for lazy loader to use
      parentIndexMapping[dim] = parentMapping;
    }
  }

  const virtualShape = newDims.map(dim => {
    if (newCoords[dim]) {
      return newCoords[dim].length;
    }
    const mapping = newOriginalIndexMapping[dim];
    if (mapping) {
      return mapping.length;
    }
    const dimIndex = dims.indexOf(dim);
    return dimIndex !== -1 ? shape[dimIndex] : 0;
  });

  const parentDims = [...dims];

  const lazyLoader = async (requestedRanges: Record<string, LazyIndexRange>) => {
    const resolved: Record<string, LazyIndexRange> = {};

    for (const dim of parentDims) {
      if (fixedOriginalIndices[dim] !== undefined) {
        resolved[dim] = fixedOriginalIndices[dim];
        continue;
      }

      // Use parent mapping to translate child virtual indices to parent virtual indices
      const mapping = parentIndexMapping[dim];
      const parentRange = indexRanges[dim];
      const requested = requestedRanges[dim];

      if (!mapping || mapping.length === 0) {
        if (typeof parentRange === 'number') {
          resolved[dim] = parentRange;
        } else if (parentRange) {
          resolved[dim] = { start: parentRange.start, stop: parentRange.stop };
        } else {
          resolved[dim] = { start: 0, stop: 0 };
        }
        continue;
      }

      const minOriginal = mapping[0];
      const maxOriginal = mapping[mapping.length - 1];
      const maxOriginalExclusive = maxOriginal + 1;

      if (requested === undefined) {
        resolved[dim] = {
          start: minOriginal,
          stop: maxOriginalExclusive
        };
        continue;
      }

      if (typeof requested === 'number') {
        if (requested >= 0 && requested < mapping.length) {
          const originalIndex = mapping[requested];
          if (originalIndex === undefined) {
            throw new Error(
              `Lazy selection index ${requested} out of bounds for dimension '${dim}' of length ${mapping.length}`
            );
          }
          resolved[dim] = originalIndex;
        } else {
          const clampedOriginal = Math.min(Math.max(requested, minOriginal), maxOriginal);
          resolved[dim] = clampedOriginal;
        }
        continue;
      }

      const startPos = requested.start ?? 0;
      const stopPos = requested.stop ?? mapping.length;

      const looksLikeChildSpace =
        startPos >= 0 &&
        startPos < mapping.length &&
        stopPos <= mapping.length &&
        stopPos >= 0;

      if (looksLikeChildSpace) {
        const clampedStart = Math.max(0, Math.min(startPos, mapping.length - 1));
        const clampedStopIdx = Math.max(
          clampedStart + 1,
          Math.min(stopPos, mapping.length)
        );

        resolved[dim] = {
          start: mapping[clampedStart],
          stop: mapping[clampedStopIdx - 1] + 1
        };
      } else {
        const clampedStartOriginal = Math.max(
          minOriginal,
          Math.min(startPos, maxOriginalExclusive)
        );
        const clampedStopOriginal = Math.max(
          clampedStartOriginal,
          Math.min(stopPos, maxOriginalExclusive)
        );

        resolved[dim] = {
          start: clampedStartOriginal,
          stop: clampedStopOriginal
        };
      }
    }

    return loader(resolved);
  };

  return {
    virtualShape,
    lazyLoader,
    dims: newDims,
    coords: newCoords,
    attrs: deepClone(attrs),
    name,
    originalIndexMapping: newOriginalIndexMapping
  };
}
