/**
 * Review test for negative UTC offsets on CF time coordinates.
 * This asserts the correct behavior, so it fails while Bug 10 is present.
 */

import { describe, test, expect } from 'vitest';
import { DataArray } from '../../src/DataArray';

describe('BUG 10: string time values with negative UTC offsets are rejected', () => {
  test('sel with an ISO string carrying a negative offset on a CF time coordinate', async () => {
    const da = new DataArray([1, 2, 3], {
      dims: ['time'],
      coords: { time: [0, 21600, 43200] }, // seconds since epoch
      attrs: { units: 'seconds since 1970-01-01', standard_name: 'time' }
    });

    // Positive offsets are detected and work fine ...
    const plus = await da.sel({ time: '1970-01-01T06:00:00+00:00' });
    expect(plus.data).toBe(2);

    // ... but negative offsets are not detected; a 'Z' gets appended producing an
    // invalid date string. 1970-01-01T01:00:00-05:00 == 1970-01-01T06:00:00Z == 21600 s
    const res = await da.sel({ time: '1970-01-01T01:00:00-05:00' });
    expect(res.data).toBe(2);
  });
});
