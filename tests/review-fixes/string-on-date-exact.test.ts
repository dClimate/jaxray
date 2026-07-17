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

  test('eager range sel with ISO string bounds matches Date bounds', async () => {
    const dates = [
      new Date('2002-02-01T00:00:00Z'),
      new Date('2002-02-11T00:00:00Z'),
      new Date('2002-02-21T00:00:00Z')
    ];
    const da = new DataArray([1, 2, 3], { dims: ['time'], coords: { time: dates } });

    const byString = await da.sel({
      time: { start: '2002-02-01T00:00:00Z', stop: '2002-02-11T00:00:00Z' }
    });
    const byDate = await da.sel({
      time: {
        start: new Date('2002-02-01T00:00:00Z'),
        stop: new Date('2002-02-11T00:00:00Z')
      }
    });

    expect(byString.data).toEqual([1, 2]);
    expect(byString.coords.time).toEqual(dates.slice(0, 2));
    expect({ data: byString.data, coords: byString.coords }).toEqual({
      data: byDate.data,
      coords: byDate.coords
    });
  });

  test('eager range sel with an unmatched ISO string bound reports not found', async () => {
    const dates = [
      new Date('2002-02-01T00:00:00Z'),
      new Date('2002-02-11T00:00:00Z'),
      new Date('2002-02-21T00:00:00Z')
    ];
    const da = new DataArray([1, 2, 3], { dims: ['time'], coords: { time: dates } });

    const selection = da.sel({
      time: { start: '2002-02-02T00:00:00Z', stop: '2002-02-11T00:00:00Z' }
    });

    await expect(selection).rejects.toThrow(/coordinate.*not found/i);
    await expect(selection).rejects.not.toThrow(/length \(0\) does not match dimension size/i);
  });
});
