import { describe, test, expect } from 'vitest';
import { DataArray } from '../../src/DataArray';

describe('BUG 11 (issue #2 variant): exact string sel on Date coordinates', () => {
  test('sel with ISO string on Date-object coordinates should match', async () => {
    const dates = [
      new Date('2002-02-01T00:00:00Z'),
      new Date('2002-02-11T00:00:00Z'),
      new Date('2002-02-21T00:00:00Z')
    ];
    const da = new DataArray([1, 2, 3], { dims: ['time'], coords: { time: dates } });

    // Works with a Date object (issue #2 fix) ...
    const byDate = await da.sel({ time: new Date('2002-02-11T00:00:00Z') });
    expect(byDate.data).toBe(2);

    // ... but the equivalent string lookup (xarray-supported) fails
    const byString = await da.sel({ time: '2002-02-11T00:00:00Z' });
    expect(byString.data).toBe(2);
  });
});
