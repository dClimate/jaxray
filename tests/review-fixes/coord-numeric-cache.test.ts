import { describe, test, expect } from 'vitest';
import { findCoordinateIndex } from '../../src/utils/coordinate-indexing';

const HOUR_MS = 60 * 60 * 1000;
const EPOCH_MS = Date.UTC(2000, 0, 1);
const TIME_ATTRS = {
  units: 'hours since 2000-01-01T00:00:00Z',
  calendar: 'proleptic_gregorian',
  standard_name: 'time'
};

function hourlyIso(index: number): string {
  return new Date(EPOCH_MS + index * HOUR_MS).toISOString();
}

describe('correctness', () => {
  test('resolves exact and nearest ISO time coordinates', () => {
    const coords = Array.from({ length: 50_000 }, (_, index) => hourlyIso(index));

    expect(findCoordinateIndex(coords, hourlyIso(0), undefined, 'time', TIME_ATTRS)).toBe(0);
    expect(findCoordinateIndex(coords, hourlyIso(25_000), undefined, 'time', TIME_ATTRS)).toBe(25_000);
    expect(findCoordinateIndex(coords, hourlyIso(49_999), undefined, 'time', TIME_ATTRS)).toBe(49_999);
    expect(findCoordinateIndex(coords, hourlyIso(12_345), undefined, 'time', TIME_ATTRS)).toBe(12_345);

    const betweenValues = new Date(EPOCH_MS + (12_345 + 0.6) * HOUR_MS).toISOString();
    expect(findCoordinateIndex(coords, betweenValues, { method: 'nearest' }, 'time', TIME_ATTRS)).toBe(12_346);
  });

  test('recomputes a coordinate array when its time units change', () => {
    const coords = Array.from({ length: 72 }, (_, index) => hourlyIso(index));
    const target = hourlyIso(30);

    expect(findCoordinateIndex(coords, target, undefined, 'time', TIME_ATTRS)).toBe(30);
    expect(findCoordinateIndex(
      coords,
      target,
      undefined,
      'time',
      { ...TIME_ATTRS, units: 'days since 2000-01-01T00:00:00Z' }
    )).toBe(30);
  });
});

describe('perf', () => {
  test('reuses numeric conversion across repeated ISO time lookups', () => {
    const coordinateCount = 50_000;
    const lookupCount = 500;
    const coords = Array.from({ length: coordinateCount }, (_, index) => hourlyIso(index));
    const targets = Array.from({ length: lookupCount }, (_, lookup) => {
      const index = Math.round(lookup * (coordinateCount - 1) / (lookupCount - 1));
      return coords[index];
    });
    const durations: number[] = [];

    for (let run = 0; run < 3; run++) {
      const start = performance.now();
      let indexSum = 0;

      for (const target of targets) {
        indexSum += findCoordinateIndex(coords, target, undefined, 'time', TIME_ATTRS);
      }

      durations.push(performance.now() - start);
      expect(indexSum).toBe(12_499_750);
    }

    const sortedDurations = [...durations].sort((a, b) => a - b);
    const median = sortedDurations[1];
    console.info(
      `[coord-numeric-cache perf] 500-lookup batches: ${durations.map(ms => ms.toFixed(2)).join(', ')} ms; median: ${median.toFixed(2)} ms`
    );

    // Calibrated on Apple M1 Max; the 1s threshold leaves generous margins
    // between the uncached and cached paths.
    expect(median).toBeLessThan(1_000);
  }, 30_000);
});
