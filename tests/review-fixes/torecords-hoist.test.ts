// RED for torecords-hoist is correctness-pinning, not failure-first, per the perf report — these pins pass before AND after the refactor and guard it.

import { describe, test, expect } from 'vitest';
import { DataArray } from '../../src/DataArray';

function makeArray(): DataArray {
  return new DataArray([
    [1.23456789, null],
    [Number.NaN, -9.87654321]
  ], {
    dims: ['latitude', 'longitude'],
    coords: {
      latitude: [10.123456789, 20.987654321],
      longitude: [-70.123456789, -60.987654321]
    }
  });
}

describe('toRecords rounding-factor hoist correctness pins', () => {
  test('preserves default precision and 2-D record order', () => {
    expect(makeArray().toRecords()).toEqual([
      { latitude: 10.123457, longitude: -70.123457, value: 1.23456789 },
      { latitude: 10.123457, longitude: -60.987654, value: null },
      { latitude: 20.987654, longitude: -70.123457, value: Number.NaN },
      { latitude: 20.987654, longitude: -60.987654, value: -9.87654321 }
    ]);
  });

  test('preserves explicit precision without rounding data values', () => {
    expect(makeArray().toRecords({ precision: 2 })).toEqual([
      { latitude: 10.12, longitude: -70.12, value: 1.23456789 },
      { latitude: 10.12, longitude: -60.99, value: null },
      { latitude: 20.99, longitude: -70.12, value: Number.NaN },
      { latitude: 20.99, longitude: -60.99, value: -9.87654321 }
    ]);
  });

  test('preserves precision zero with null and NaN data values', () => {
    expect(makeArray().toRecords({ precision: 0 })).toEqual([
      { latitude: 10, longitude: -70, value: 1.23456789 },
      { latitude: 10, longitude: -61, value: null },
      { latitude: 21, longitude: -70, value: Number.NaN },
      { latitude: 21, longitude: -61, value: -9.87654321 }
    ]);
  });
});
