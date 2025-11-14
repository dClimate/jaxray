/**
 * Coordinate indexing utilities for DataArray
 * Handles coordinate lookups with various selection methods (nearest, ffill, bfill)
 * and optimizations like binary search for sorted coordinates
 */

import { CoordinateValue, DimensionName, SelectionOptions } from '../types.js';
import { isTimeCoordinate, parseCFTimeUnits } from '../time/cf-time.js';

/**
 * Find the index of a coordinate value in a coordinate array
 * Supports exact match, nearest, forward fill, and backward fill methods
 * Optimized with O(1) arithmetic for evenly-spaced coordinates and O(log n) binary search for sorted coordinates
 */
export function findCoordinateIndex(
  coords: CoordinateValue[],
  value: CoordinateValue,
  options: SelectionOptions | undefined,
  dim: DimensionName,
  dimAttrs?: any
): number {
  const method = options?.method;
  const tolerance = options?.tolerance;

  const units = dimAttrs?.units as string | undefined;
  const timeLike = isTimeCoordinate(dimAttrs);
  let parsedUnits: ReturnType<typeof parseCFTimeUnits> | null | undefined;

  const parseUnits = () => {
    if (parsedUnits === undefined) {
      parsedUnits = units ? parseCFTimeUnits(units) : null;
    }
    return parsedUnits;
  };

  const convertDateToNumeric = (date: Date): number | undefined => {
    const parsed = parseUnits();
    if (!parsed) return undefined;
    const { unit, referenceDate } = parsed;
    const diff = date.getTime() - referenceDate.getTime();
    switch (unit) {
      case 'second':
        return diff / 1000;
      case 'minute':
        return diff / (60 * 1000);
      case 'hour':
        return diff / (60 * 60 * 1000);
      case 'day':
        return diff / (24 * 60 * 60 * 1000);
      case 'week':
        return diff / (7 * 24 * 60 * 60 * 1000);
      case 'month':
        return diff / (30 * 24 * 60 * 60 * 1000);
      case 'year':
        return diff / (365.25 * 24 * 60 * 60 * 1000);
      default:
        return diff / 1000;
    }
  };

  const convertValueToNumeric = (val: CoordinateValue): number | undefined => {
    if (typeof val === 'number') return val;
    if (typeof val === 'bigint') return Number(val);
    if (val instanceof Date) return convertDateToNumeric(val);
    if (typeof val === 'string' && timeLike && units) {
      const parsed = parseUnits();
      if (!parsed) return undefined;
      let inputStr = val;
      if (!inputStr.endsWith('Z') && !inputStr.includes('+')) {
        inputStr = `${inputStr}Z`;
      }
      const asDate = new Date(inputStr);
      if (Number.isNaN(asDate.getTime())) {
        throw new Error(`Invalid date string: '${val}'`);
      }
      return convertDateToNumeric(asDate);
    }
    return undefined;
  };

  let numericValue: number | undefined = convertValueToNumeric(value);

  if (numericValue === undefined) {
    if (typeof value === 'string') {
      return findIndexFallback(coords, value, method, tolerance, dim);
    }
    return findIndexFallback(coords, value, method, tolerance, dim);
  }

  const numericCoords: number[] = [];
  let canUseNumeric = true;
  for (const coord of coords) {
    const converted = convertValueToNumeric(coord);
    if (converted === undefined) {
      canUseNumeric = false;
      break;
    }
    numericCoords.push(converted);
  }

  if (canUseNumeric && numericCoords.length >= 2) {
    const numCoords = numericCoords;
    const min = numCoords[0];
    const step = numCoords.length > 1 ? (numCoords[1] - numCoords[0]) : 1;

    // Check if coordinates are evenly spaced (with small tolerance for floating point)
    const isEvenlySpaced = numCoords.length <= 2 || numCoords.every((coord, i) => {
      if (i === 0) return true;
      const expectedValue = min + i * step;
      return Math.abs(coord - expectedValue) < Math.abs(step) * 1e-6;
    });

    if (isEvenlySpaced && Math.abs(step) > 1e-10) {
      // Use arithmetic calculation (O(1) instead of O(n))
      const rawIndex = (numericValue - min) / step;

      let index: number;
      switch (method) {
        case 'nearest':
          index = Math.round(rawIndex);
          break;
        case 'ffill':
        case 'pad':
          index = Math.floor(rawIndex);
          break;
        case 'bfill':
        case 'backfill':
          index = Math.ceil(rawIndex);
          break;
        case null:
        case undefined:
          // Exact match - check if close to integer
          const roundedIndex = Math.round(rawIndex);
          const indexTolerance = tolerance !== undefined ? tolerance / Math.abs(step) : 1e-3;
          if (Math.abs(rawIndex - roundedIndex) > indexTolerance) {
            throw new Error(`Coordinate value '${value}' not found in dimension '${dim}' (no exact match)`);
          }
          index = roundedIndex;
          break;
        default:
          throw new Error(`Unknown method: ${method}`);
      }

      // Bounds check
      if (index < 0 || index >= coords.length) {
        if (method === 'ffill' || method === 'pad') {
          throw new Error(`No coordinate <= ${numericValue} for forward fill`);
        } else if (method === 'bfill' || method === 'backfill') {
          throw new Error(`No coordinate >= ${numericValue} for backward fill`);
        } else if (tolerance !== undefined) {
          throw new Error(`No coordinate within tolerance ${tolerance} of value ${numericValue}`);
        }
        throw new Error(`Coordinate value '${value}' out of bounds for dimension '${dim}'`);
      }

      // Optional: verify tolerance if specified
      if (tolerance !== undefined) {
        const actualValue = numCoords[index];
        if (Math.abs(actualValue - numericValue) > tolerance) {
          throw new Error(`No coordinate within tolerance ${tolerance} of value ${numericValue}`);
        }
      }

      return index;
    }

    // Not evenly spaced but we have numeric coords - use fallback with numeric arrays
    return findIndexFallback(numericCoords, numericValue, method, tolerance, dim);
  }

  // Fallback to linear search for non-numeric coordinates
  return findIndexFallback(coords, value, method, tolerance, dim);
}

/**
 * Fallback method using linear search (original indexOf-based approach)
 */
export function findIndexFallback(
  coords: CoordinateValue[],
  value: CoordinateValue,
  method?: string,
  tolerance?: number,
  dim?: string
): number {
  // Apply selection method
  if (method === 'nearest') {
    return findNearestIndex(coords, value, tolerance, dim);
  } else if (method === 'ffill' || method === 'pad') {
    return findFfillIndex(coords, value, tolerance, dim);
  } else if (method === 'bfill' || method === 'backfill') {
    return findBfillIndex(coords, value, tolerance, dim);
  }

  // Default exact match
  const index = coords.indexOf(value);
  if (index === -1) {
    throw new Error(`Coordinate value '${value}' not found in dimension`);
  }

  return index;
}

/**
 * Find nearest coordinate index (optimized with binary search for sorted coords)
 */
export function findNearestIndex(
  coords: CoordinateValue[],
  value: CoordinateValue,
  tolerance?: number,
  dim?: string
): number {
  if (typeof value !== 'number' || !coords.every(c => typeof c === 'number')) {
    throw new Error('Nearest neighbor lookup requires numeric coordinates');
  }

  const numCoords = coords as number[];

  // Use binary search if coordinates are sorted
  if (dim && numCoords.length > 20) {
    if (isCoordsSorted(numCoords)) {
      const ascending = numCoords.length < 2 || numCoords[1] >= numCoords[0];
      const closestIndex = binarySearchNearest(numCoords, value, ascending);
      const minDiff = Math.abs(numCoords[closestIndex] - value);

      if (tolerance !== undefined && minDiff > tolerance) {
        throw new Error(`No coordinate within tolerance ${tolerance} of value ${value}`);
      }

      return closestIndex;
    }
  }

  // Fallback to linear search
  let closestIndex = 0;
  let minDiff = Math.abs(numCoords[0] - value);

  for (let i = 1; i < numCoords.length; i++) {
    const diff = Math.abs(numCoords[i] - value);
    if (diff < minDiff) {
      minDiff = diff;
      closestIndex = i;
    }
  }

  if (tolerance !== undefined && minDiff > tolerance) {
    throw new Error(`No coordinate within tolerance ${tolerance} of value ${value}`);
  }

  return closestIndex;
}

/**
 * Find forward fill index (last valid index <= value) - optimized with binary search
 */
export function findFfillIndex(
  coords: CoordinateValue[],
  value: CoordinateValue,
  tolerance?: number,
  dim?: string
): number {
  if (typeof value !== 'number' || !coords.every(c => typeof c === 'number')) {
    throw new Error('Forward fill requires numeric coordinates');
  }

  const numCoords = coords as number[];

  // Use binary search if coordinates are sorted
  if (dim && numCoords.length > 20) {
    if (isCoordsSorted(numCoords)) {
      const ascending = numCoords.length < 2 || numCoords[1] >= numCoords[0];
      const lastValidIndex = binarySearchFfill(numCoords, value, ascending);

      if (lastValidIndex === -1) {
        throw new Error(`No coordinate <= ${value} for forward fill`);
      }

      const minDiff = value - numCoords[lastValidIndex];
      if (tolerance !== undefined && minDiff > tolerance) {
        throw new Error(`No coordinate within tolerance ${tolerance} of value ${value}`);
      }

      return lastValidIndex;
    }
  }

  // Fallback to linear search
  let lastValidIndex = -1;
  let minDiff = Infinity;

  for (let i = 0; i < numCoords.length; i++) {
    const coordValue = numCoords[i];
    if (coordValue <= value) {
      const diff = value - coordValue;
      if (diff < minDiff) {
        minDiff = diff;
        lastValidIndex = i;
      }
    }
  }

  if (lastValidIndex === -1) {
    throw new Error(`No coordinate <= ${value} for forward fill`);
  }

  if (tolerance !== undefined && minDiff > tolerance) {
    throw new Error(`No coordinate within tolerance ${tolerance} of value ${value}`);
  }

  return lastValidIndex;
}

/**
 * Find backward fill index (first valid index >= value) - optimized with binary search
 */
export function findBfillIndex(
  coords: CoordinateValue[],
  value: CoordinateValue,
  tolerance?: number,
  dim?: string
): number {
  if (typeof value !== 'number' || !coords.every(c => typeof c === 'number')) {
    throw new Error('Backward fill requires numeric coordinates');
  }

  const numCoords = coords as number[];

  // Use binary search if coordinates are sorted
  if (dim && numCoords.length > 20) {
    if (isCoordsSorted(numCoords)) {
      const ascending = numCoords.length < 2 || numCoords[1] >= numCoords[0];
      const firstValidIndex = binarySearchBfill(numCoords, value, ascending);

      if (firstValidIndex === -1) {
        throw new Error(`No coordinate >= ${value} for backward fill`);
      }

      const minDiff = numCoords[firstValidIndex] - value;
      if (tolerance !== undefined && minDiff > tolerance) {
        throw new Error(`No coordinate within tolerance ${tolerance} of value ${value}`);
      }

      return firstValidIndex;
    }
  }

  // Fallback to linear search
  let firstValidIndex = -1;
  let minDiff = Infinity;

  for (let i = 0; i < numCoords.length; i++) {
    const coordValue = numCoords[i];
    if (coordValue >= value) {
      const diff = coordValue - value;
      if (diff < minDiff) {
        minDiff = diff;
        firstValidIndex = i;
      }
    }
  }

  if (firstValidIndex === -1) {
    throw new Error(`No coordinate >= ${value} for backward fill`);
  }

  if (tolerance !== undefined && minDiff > tolerance) {
    throw new Error(`No coordinate within tolerance ${tolerance} of value ${value}`);
  }

  return firstValidIndex;
}

/**
 * Check if numeric coordinates are sorted
 */
export function isCoordsSorted(coords: number[]): boolean {
  let ascending = true;
  let descending = true;

  for (let i = 1; i < coords.length; i++) {
    if (coords[i] < coords[i - 1]) ascending = false;
    if (coords[i] > coords[i - 1]) descending = false;
    if (!ascending && !descending) break;
  }

  return ascending || descending;
}

/**
 * Binary search for nearest value in sorted array - O(log n)
 */
export function binarySearchNearest(sorted: number[], target: number, ascending: boolean = true): number {
  let left = 0;
  let right = sorted.length - 1;
  let closest = 0;
  let minDiff = Math.abs(sorted[0] - target);

  while (left <= right) {
    const mid = Math.floor((left + right) / 2);
    const diff = Math.abs(sorted[mid] - target);

    if (diff < minDiff) {
      minDiff = diff;
      closest = mid;
    }

    if (sorted[mid] === target) {
      return mid;
    }

    if (ascending) {
      if (sorted[mid] < target) {
        left = mid + 1;
      } else {
        right = mid - 1;
      }
    } else {
      if (sorted[mid] > target) {
        left = mid + 1;
      } else {
        right = mid - 1;
      }
    }
  }

  return closest;
}

/**
 * Binary search for forward fill in sorted array - O(log n)
 */
export function binarySearchFfill(sorted: number[], target: number, ascending: boolean = true): number {
  let result = -1;

  if (ascending) {
    // Find largest value <= target
    let left = 0;
    let right = sorted.length - 1;

    while (left <= right) {
      const mid = Math.floor((left + right) / 2);
      if (sorted[mid] <= target) {
        result = mid;
        left = mid + 1; // Continue searching right for larger values still <= target
      } else {
        right = mid - 1;
      }
    }
  } else {
    // Descending: find smallest value <= target (happens later in array)
    let left = 0;
    let right = sorted.length - 1;

    while (left <= right) {
      const mid = Math.floor((left + right) / 2);
      if (sorted[mid] <= target) {
        result = mid;
        right = mid - 1; // Continue searching left for smaller values still <= target
      } else {
        left = mid + 1;
      }
    }
  }

  return result;
}

/**
 * Binary search for backward fill in sorted array - O(log n)
 */
export function binarySearchBfill(sorted: number[], target: number, ascending: boolean = true): number {
  let result = -1;

  if (ascending) {
    // Find smallest value >= target
    let left = 0;
    let right = sorted.length - 1;

    while (left <= right) {
      const mid = Math.floor((left + right) / 2);
      if (sorted[mid] >= target) {
        result = mid;
        right = mid - 1; // Continue searching left for smaller values still >= target
      } else {
        left = mid + 1;
      }
    }
  } else {
    // Descending: find largest value >= target (happens earlier in array)
    let left = 0;
    let right = sorted.length - 1;

    while (left <= right) {
      const mid = Math.floor((left + right) / 2);
      if (sorted[mid] >= target) {
        result = mid;
        left = mid + 1; // Continue searching right for larger values still >= target
      } else {
        right = mid - 1;
      }
    }
  }

  return result;
}
