import { describe, test, expect } from 'vitest';
import { rolling1D } from '../../src/utils/rolling-operations';

function mulberry32(seed: number): () => number {
  return () => {
    seed |= 0;
    seed = (seed + 0x6d2b79f5) | 0;
    let value = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    value = (value + Math.imul(value ^ (value >>> 7), 61 | value)) ^ value;
    return ((value ^ (value >>> 14)) >>> 0) / 4294967296;
  };
}

function naiveCenteredMean(
  values: Array<number | null>,
  window: number,
  minPeriods = window
): Array<number | null> {
  const result: Array<number | null> = new Array(values.length).fill(null);
  const half = Math.floor((window - 1) / 2);

  for (let i = 0; i < values.length; i++) {
    const unclampedStart = i - half;
    const start = Math.max(unclampedStart, 0);
    const end = Math.min(unclampedStart + window - 1, values.length - 1);
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
      result[i] = sum / count;
    }
  }

  return result;
}

function expectValuesClose(
  actual: ReturnType<typeof rolling1D>,
  expected: Array<number | null>
): void {
  expect(actual).toHaveLength(expected.length);
  expected.forEach((expectedValue, index) => {
    if (expectedValue === null) {
      expect(actual[index]).toBeNull();
    } else {
      expect(actual[index]).toBeCloseTo(expectedValue, 12);
    }
  });
}

describe('centered rolling mean', () => {
  test('matches a naive centered-window reference', () => {
    const random = mulberry32(0x5eed1234);
    const values: Array<number | null> = Array.from(
      { length: 200 },
      () => random() * 200 - 100
    );
    values[19] = Number.NaN;
    values[73] = null;
    values[151] = Number.NaN;
    const window = 11;

    const defaultMinPeriods = rolling1D(values, window, { center: true }, 'mean');
    expectValuesClose(defaultMinPeriods, naiveCenteredMean(values, window));

    const skipNulls = rolling1D(
      values,
      window,
      { center: true, minPeriods: 5 },
      'mean'
    );
    expectValuesClose(skipNulls, naiveCenteredMean(values, window, 5));
  });
});

describe('perf', () => {
  test('centered rolling mean scales as a sliding window', () => {
    const n = 1_000_000;
    const window = 168;
    const values = Array.from({ length: n }, (_, index) =>
      ((index * 17) % 1000) / 1000
    );
    const durations: number[] = [];

    for (let run = 0; run < 3; run++) {
      const start = performance.now();
      const result = rolling1D(values, window, { center: true }, 'mean');
      durations.push(performance.now() - start);
      expect(result).toHaveLength(n);
    }

    durations.sort((a, b) => a - b);
    const median = durations[1];

    // Apple M1 Max: current O(n*w) code is ~186ms; fixed sliding-window code is
    // ~25ms. A 100ms threshold leaves generous margin between the two paths.
    expect(median).toBeLessThan(100);
  }, 30_000);
});
