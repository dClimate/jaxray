/**
 * Lazy selection utilities for DataArray
 * Handles selection operations on lazy-loaded arrays without materializing the entire dataset
 */

import {
  Selection,
  DimensionName,
  Coordinates,
  SelectionOptions,
  LazyIndexRange,
  Attributes,
  CoordinateValue
} from '../types.js';
import { findCoordinateIndex } from './coordinate-indexing.js';
import { selectMultipleAtDimension } from './data-operations.js';

const DISCRETE_FETCH_GAP = 16;

interface DiscreteFetchRun {
  start: number;
  stop: number;
}

function createDiscreteFetchRuns(indices: number[]): DiscreteFetchRun[] {
  const sortedIndices = [...new Set(indices)].sort((a, b) => a - b);
  const runs: DiscreteFetchRun[] = [];

  for (const index of sortedIndices) {
    const lastRun = runs[runs.length - 1];
    if (lastRun && index - lastRun.stop <= DISCRETE_FETCH_GAP) {
      lastRun.stop = index + 1;
    } else {
      runs.push({ start: index, stop: index + 1 });
    }
  }

  return runs;
}

function stitchDiscreteFetchRuns(
  results: any[],
  runs: DiscreteFetchRun[],
  indices: number[],
  dimIndex: number
): any {
  const locationsByIndex = new Map<number, { runIndex: number; offset: number }>();
  let runIndex = 0;
  for (const index of [...new Set(indices)].sort((a, b) => a - b)) {
    while (index >= runs[runIndex].stop) runIndex++;
    locationsByIndex.set(index, { runIndex, offset: index - runs[runIndex].start });
  }
  const locations = indices.map(index => locationsByIndex.get(index)!);

  const stitchAtDimension = (runResults: any[], currentDimIndex: number): any => {
    if (currentDimIndex === 0) {
      return locations.map(({ runIndex, offset }) => runResults[runIndex][offset]);
    }
    return runResults[0].map((_: any, index: number) =>
      stitchAtDimension(runResults.map(result => result[index]), currentDimIndex - 1)
    );
  };

  return stitchAtDimension(results, dimIndex);
}

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
  positional?: boolean;
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
    positional = false,
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
  // Track which dimensions use discrete (non-contiguous) index selection.
  // After the loader fetches a contiguous range, these dimensions need post-fetch extraction.
  const discreteSelectionDimensions = new Set<DimensionName>();

  // Get coordinate attributes for time conversion
  const coordAttrs = (attrs as any)?._coordAttrs;

  for (let i = 0; i < dims.length; i++) {
    const dim = dims[i];
    const sel = selection[dim];
    const dimAttrs = coordAttrs?.[dim] || attrs;
    if (sel === undefined) {
      const length = shape[i];

      // An untouched dimension without a prior mapping is already in parent
      // space. Preserve that identity implicitly instead of allocating O(n)
      // mapping arrays.
      if (!originalIndexMapping || !originalIndexMapping[dim]) {
        indexRanges[dim] = { start: 0, stop: length };
        newDims.push(dim);
        newCoords[dim] = coords[dim];
        continue;
      }

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
      const index = positional
        ? sel as number
        : findCoordinateIndex(coords[dim], sel, options, dim, dimAttrs);
      indexRanges[dim] = index;
      fixedOriginalIndices[dim] = index;
    } else if (Array.isArray(sel)) {
      // xarray-compatible: array selection picks discrete points, not a contiguous range.
      const indices = positional
        ? sel as number[]
        : sel.map(v => findCoordinateIndex(coords[dim], v, options, dim, dimAttrs));

      // Parent mapping: only the exact requested indices (discrete, possibly non-contiguous)
      const parentMapping = indices;

      // Original mapping: map through to original space
      const mapping = parentMapping.map(parentIdx =>
        mapIndexToOriginal(originalIndexMapping, dim, parentIdx)
      );

      // Keep the enclosing range as a fallback; the loader wrapper splits sparse requests below.
      const minIdx = Math.min(...indices);
      const maxIdx = Math.max(...indices);
      indexRanges[dim] = {
        start: minIdx,
        stop: maxIdx + 1
      };

      newDims.push(dim);
      // Only include coordinates for the exact requested points
      newCoords[dim] = indices.map(idx => coords[dim][idx]);
      newOriginalIndexMapping[dim] = mapping;
      parentIndexMapping[dim] = parentMapping;
      discreteSelectionDimensions.add(dim);
    } else if (sel && typeof sel === 'object' && ('start' in sel || 'stop' in sel)) {
      const { start, stop } = sel as { start?: CoordinateValue; stop?: CoordinateValue };
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
    const discreteSelectionOffsets: { [dim: string]: number[] } = {};
    const discreteFetchCandidates: {
      dim: DimensionName;
      indices: number[];
      runs: DiscreteFetchRun[];
    }[] = [];

    for (const dim of parentDims) {
      if (fixedOriginalIndices[dim] !== undefined) {
        resolved[dim] = fixedOriginalIndices[dim];
        continue;
      }

      // Use parent mapping to translate child virtual indices to parent virtual indices
      const mapping = parentIndexMapping[dim];
      const parentRange = indexRanges[dim];
      const requested = requestedRanges[dim];

      if (!mapping) {
        // Identity dimensions have no mapping to translate. Pass requests
        // through in parent space, clamped to the dimension extent.
        if (typeof parentRange === 'number') {
          resolved[dim] = parentRange;
        } else if (requested === undefined) {
          resolved[dim] = parentRange
            ? { start: parentRange.start, stop: parentRange.stop }
            : { start: 0, stop: 0 };
        } else if (typeof requested === 'number') {
          resolved[dim] = parentRange
            ? Math.min(Math.max(requested, parentRange.start), parentRange.stop - 1)
            : requested;
        } else {
          const start = parentRange?.start ?? 0;
          const stop = parentRange?.stop ?? requested.stop;
          resolved[dim] = {
            start: Math.max(start, Math.min(requested.start ?? start, stop)),
            stop: Math.max(start, Math.min(requested.stop ?? stop, stop))
          };
        }
        continue;
      }

      if (mapping.length === 0) {
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
        if (discreteSelectionDimensions.has(dim)) {
          const rangeStart = Math.min(...mapping);
          const rangeStop = Math.max(...mapping) + 1;
          resolved[dim] = { start: rangeStart, stop: rangeStop };
          discreteSelectionOffsets[dim] = mapping.map(index => index - rangeStart);
          discreteFetchCandidates.push({
            dim,
            indices: mapping,
            runs: createDiscreteFetchRuns(mapping)
          });
          continue;
        }
        resolved[dim] = {
          start: minOriginal,
          stop: maxOriginalExclusive
        };
        continue;
      }

      if (typeof requested === 'number') {
        const originalIndex = mapping[requested];
        if (requested < 0 || requested >= mapping.length || originalIndex === undefined) {
          throw new Error(
            `Lazy selection index ${requested} out of bounds for dimension '${dim}' of length ${mapping.length}`
          );
        }
        resolved[dim] = originalIndex;
        continue;
      }

      const startPos = requested.start ?? 0;
      const stopPos = requested.stop ?? mapping.length;

      // All requests to a lazy loader are in child (virtual) space
      // The mapping translates child indices to parent indices
      const clampedStart = Math.max(0, Math.min(startPos, mapping.length - 1));
      const clampedStopIdx = Math.max(
        clampedStart + 1,
        Math.min(stopPos, mapping.length)
      );

      if (discreteSelectionDimensions.has(dim)) {
        const selectedMapping = mapping.slice(clampedStart, clampedStopIdx);
        const resolvedStart = Math.min(...selectedMapping);
        resolved[dim] = {
          start: resolvedStart,
          stop: Math.max(...selectedMapping) + 1
        };
        discreteSelectionOffsets[dim] = selectedMapping.map(index => index - resolvedStart);
        discreteFetchCandidates.push({
          dim,
          indices: selectedMapping,
          runs: createDiscreteFetchRuns(selectedMapping)
        });
      } else {
        resolved[dim] = {
          start: mapping[clampedStart],
          stop: mapping[clampedStopIdx - 1] + 1
        };
      }
    }

    const splitCandidate = discreteFetchCandidates
      .filter(candidate => candidate.runs.length > 1)
      .reduce<typeof discreteFetchCandidates[number] | undefined>((best, candidate) => {
        const range = resolved[candidate.dim];
        if (typeof range === 'number') return best;
        const savedElements = (range.stop - range.start) - candidate.runs.reduce(
          (total, run) => total + run.stop - run.start,
          0
        );
        if (!best) return candidate;
        const bestRange = resolved[best.dim];
        if (typeof bestRange === 'number') return candidate;
        const bestSavedElements = (bestRange.stop - bestRange.start) - best.runs.reduce(
          (total, run) => total + run.stop - run.start,
          0
        );
        return savedElements > bestSavedElements ? candidate : best;
      }, undefined);

    // For dimensions with discrete (non-contiguous) array selections, the loader
    // returned a contiguous range. Extract only the exact requested indices.
    // Process in reverse dim order so earlier extractions don't shift later dim positions.
    const resultDims = newDims;
    const extractDiscreteSelections = (loaded: any): any => {
      let result = loaded;
      for (let d = resultDims.length - 1; d >= 0; d--) {
        const dim = resultDims[d];
        const offsets = discreteSelectionOffsets[dim];
        if (!offsets) continue;
        if (typeof requestedRanges[dim] === 'number') continue;

        // Check if offsets are already contiguous (no extraction needed)
        const isContiguous = offsets.every((v, i) => i === 0 || v === offsets[i - 1] + 1);
        if (isContiguous && offsets.length === (Math.max(...offsets) - Math.min(...offsets) + 1)) continue;

        const droppedPrecedingDims = resultDims
          .slice(0, d)
          .filter(precedingDim => typeof requestedRanges[precedingDim] === 'number')
          .length;
        result = selectMultipleAtDimension(result, d - droppedPrecedingDims, offsets);
      }
      return result;
    };

    if (!splitCandidate) {
      return extractDiscreteSelections(await loader(resolved));
    }

    delete discreteSelectionOffsets[splitCandidate.dim];
    const runResults = await Promise.all(splitCandidate.runs.map(run => loader({
      ...resolved,
      [splitCandidate.dim]: run
    })));
    const dimPosition = resultDims.indexOf(splitCandidate.dim);
    const droppedPrecedingDims = resultDims
      .slice(0, dimPosition)
      .filter(precedingDim => typeof requestedRanges[precedingDim] === 'number')
      .length;

    return stitchDiscreteFetchRuns(
      runResults.map(extractDiscreteSelections),
      splitCandidate.runs,
      splitCandidate.indices,
      dimPosition - droppedPrecedingDims
    );
  };

  return {
    virtualShape,
    lazyLoader,
    dims: newDims,
    coords: newCoords,
    // Top-level attrs remain isolated between results. Nested metadata is
    // intentionally shared because Zarr attrs embed full coordinate arrays.
    attrs: { ...attrs },
    name,
    originalIndexMapping: newOriginalIndexMapping
  };
}
