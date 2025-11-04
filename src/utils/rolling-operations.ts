/**
 * Rolling window operations for time series analysis
 * Handles moving window calculations like rolling mean and sum
 */

import { NDArray, DataValue, RollingOptions } from '../types.js';

/**
 * Apply rolling window operation along a specific dimension
 */
export function applyRolling(
  data: NDArray,
  dimIndex: number,
  window: number,
  options: RollingOptions,
  reducer: 'mean' | 'sum'
): NDArray {
  const apply = (input: any, axis: number): any => {
    if (!Array.isArray(input)) {
      return input;
    }

    if (axis === dimIndex) {
      return rolling1D(input as DataValue[], window, options, reducer);
    }

    return input.map(child => apply(child, axis + 1));
  };

  return apply(data, 0) as NDArray;
}

/**
 * Apply rolling window operation to a 1D array
 * Optimized with sliding window algorithm for non-centered windows
 */
export function rolling1D(
  values: DataValue[],
  window: number,
  options: RollingOptions,
  reducer: 'mean' | 'sum'
): DataValue[] {
  const len = values.length;
  const result: DataValue[] = new Array(len).fill(null);
  const center = options.center ?? false;
  const minPeriods = options.minPeriods ?? window;
  const normalizedWindow = window <= 0 ? 1 : window;

  // For centered windows or when NaNs might be present, use optimized approach
  if (!center) {
    // Use sliding window algorithm for non-centered windows - O(n) instead of O(n*w)
    let sum = 0;
    let count = 0;

    for (let i = 0; i < len; i++) {
      // Add new value entering the window
      const value = values[i];
      if (typeof value === 'number' && !Number.isNaN(value)) {
        sum += value;
        count++;
      }

      // Remove old value leaving the window
      const oldIdx = i - normalizedWindow;
      if (oldIdx >= 0) {
        const oldValue = values[oldIdx];
        if (typeof oldValue === 'number' && !Number.isNaN(oldValue)) {
          sum -= oldValue;
          count--;
        }
      }

      // Check if we have enough valid values
      if (count >= minPeriods && count > 0) {
        result[i] = reducer === 'sum' ? sum : sum / count;
      }
    }

    return result;
  }

  // Fallback to original algorithm for centered windows
  // (more complex due to asymmetric window boundaries)
  for (let i = 0; i < len; i++) {
    let start: number;
    let end: number;

    const half = Math.floor((normalizedWindow - 1) / 2);
    start = i - half;
    end = start + normalizedWindow - 1;

    start = Math.max(start, 0);
    end = Math.min(end, len - 1);

    if (end < start) {
      continue;
    }

    let sum = 0;
    let count = 0;

    for (let j = start; j <= end; j++) {
      const value = values[j];
      if (typeof value === 'number' && !Number.isNaN(value)) {
        sum += value;
        count++;
      }
    }

    if (count >= minPeriods && count > 0) {
      result[i] = reducer === 'sum' ? sum : sum / count;
    }
  }

  return result;
}
