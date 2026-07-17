import { describe, test, expect } from 'vitest';
import { DataArray } from '../../src/DataArray';

describe('rolling() over a non-innermost dimension', () => {
  test('rolling mean along time should preserve shape, dimensions, and coordinates', () => {
    const da = new DataArray([[1, 2], [3, 4], [5, 6]], {
      dims: ['time', 'x'],
      coords: { time: [0, 1, 2], x: [0, 1] }
    });

    const rolled = da.rolling('time', 2).mean();

    expect(rolled.data).toEqual([[null, null], [2, 3], [4, 5]]);
    expect(rolled.dims).toEqual(['time', 'x']);
    expect(rolled.coords).toEqual({ time: [0, 1, 2], x: [0, 1] });
  });
});
