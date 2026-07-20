process.env.TZ = 'America/Chicago';

import { describe, expect, test } from 'vitest';
import { DataArray } from '../../src/DataArray';

const HOURS_IN_DAY = 24;

function utcDateCoords(day: number): Date[] {
  return Array.from(
    { length: HOURS_IN_DAY },
    (_, hour) => new Date(Date.UTC(2021, 0, day, hour))
  );
}

function zonelessStringCoords(day: number): string[] {
  return Array.from(
    { length: HOURS_IN_DAY },
    (_, hour) => `2021-01-${String(day).padStart(2, '0')}T${String(hour).padStart(2, '0')}:00:00`
  );
}

function utcHourValues(day: number): number[] {
  return utcDateCoords(day).map(date => date.getUTCHours());
}

describe('zone-less datetime strings use UTC in coordinate lookups', () => {
  test('test runtime honors the non-UTC timezone pin', () => {
    expect(new Date('2021-01-01T12:00:00').getTimezoneOffset()).not.toBe(0);
  });

  test('sel exact interprets a zone-less label as UTC against Date.UTC coordinates', async () => {
    const da = new DataArray(utcHourValues(15), {
      dims: ['time'],
      coords: { time: utcDateCoords(15) }
    });

    const result = await da.sel({ time: '2021-01-15T12:00:00' });

    expect(result.data).toBe(new Date(Date.UTC(2021, 0, 15, 12)).getUTCHours());
  });

  test('sel nearest interprets a zone-less label as UTC against hourly Date coordinates', async () => {
    const da = new DataArray(utcHourValues(1), {
      dims: ['time'],
      coords: { time: utcDateCoords(1) }
    });

    const result = await da.sel(
      { time: '2021-01-01T02:20:00' },
      { method: 'nearest' }
    );

    expect(result.data).toBe(new Date(Date.UTC(2021, 0, 1, 2)).getUTCHours());
  });

  test('sel ffill interprets a zone-less label as UTC against hourly Date coordinates', async () => {
    const da = new DataArray(utcHourValues(1), {
      dims: ['time'],
      coords: { time: utcDateCoords(1) }
    });

    const result = await da.sel(
      { time: '2021-01-01T02:20:00' },
      { method: 'ffill' }
    );

    expect(result.data).toBe(new Date(Date.UTC(2021, 0, 1, 2)).getUTCHours());
  });

  test('sel bfill interprets a zone-less label as UTC against hourly Date coordinates', async () => {
    const da = new DataArray(utcHourValues(1), {
      dims: ['time'],
      coords: { time: utcDateCoords(1) }
    });

    const result = await da.sel(
      { time: '2021-01-01T02:20:00' },
      { method: 'bfill' }
    );

    expect(result.data).toBe(new Date(Date.UTC(2021, 0, 1, 3)).getUTCHours());
  });

  test('sel exact interprets zone-less coordinates as UTC for an explicitly zoned label', async () => {
    const da = new DataArray(utcHourValues(1), {
      dims: ['time'],
      coords: { time: zonelessStringCoords(1) }
    });

    const result = await da.sel({ time: '2021-01-01T08:00:00Z' });

    expect(result.data).toBe(new Date(Date.UTC(2021, 0, 1, 8)).getUTCHours());
  });

  test('sel nearest interprets zone-less coordinates as UTC for a Date label', async () => {
    const da = new DataArray(utcHourValues(1), {
      dims: ['time'],
      coords: { time: zonelessStringCoords(1) }
    });

    const result = await da.sel(
      { time: new Date(Date.UTC(2021, 0, 1, 8, 20)) },
      { method: 'nearest' }
    );

    expect(result.data).toBe(new Date(Date.UTC(2021, 0, 1, 8)).getUTCHours());
  });

  test('sel ffill interprets zone-less coordinates as UTC for an explicitly zoned label', async () => {
    const da = new DataArray(utcHourValues(1), {
      dims: ['time'],
      coords: { time: zonelessStringCoords(1) }
    });

    const result = await da.sel(
      { time: '2021-01-01T08:20:00Z' },
      { method: 'ffill' }
    );

    expect(result.data).toBe(new Date(Date.UTC(2021, 0, 1, 8)).getUTCHours());
  });

  test('sel bfill interprets zone-less coordinates as UTC for a Date label', async () => {
    const da = new DataArray(utcHourValues(1), {
      dims: ['time'],
      coords: { time: zonelessStringCoords(1) }
    });

    const result = await da.sel(
      { time: new Date(Date.UTC(2021, 0, 1, 8, 20)) },
      { method: 'bfill' }
    );

    expect(result.data).toBe(new Date(Date.UTC(2021, 0, 1, 9)).getUTCHours());
  });

  test('sel slice interprets zone-less endpoints as an inclusive UTC window', async () => {
    const dates = utcDateCoords(1);
    const da = new DataArray(utcHourValues(1), {
      dims: ['time'],
      coords: { time: dates }
    });

    const result = await da.sel({
      time: {
        start: '2021-01-01T00:00:00',
        stop: '2021-01-01T05:00:00'
      }
    });

    expect(result.data).toEqual(utcHourValues(1).slice(0, 6));
    expect(result.coords.time).toEqual(dates.slice(0, 6));
  });

  test('sel exact leaves an explicit UTC zone unchanged', async () => {
    const da = new DataArray(utcHourValues(15), {
      dims: ['time'],
      coords: { time: utcDateCoords(15) }
    });

    const result = await da.sel({ time: '2021-01-15T12:00:00Z' });

    expect(result.data).toBe(new Date(Date.UTC(2021, 0, 15, 12)).getUTCHours());
  });

  test('sel exact and nearest interpret a lowercase-t label as UTC', async () => {
    const da = new DataArray(utcHourValues(1), {
      dims: ['time'],
      coords: { time: utcDateCoords(1) }
    });

    const exact = await da.sel({ time: '2021-01-01t12:00:00' });
    const nearest = await da.sel(
      { time: '2021-01-01t12:20:00' },
      { method: 'nearest' }
    );

    expect(exact.data).toBe(12);
    expect(nearest.data).toBe(12);
  });

  test('sel exact matches a Date label against zone-less string coordinates', async () => {
    const da = new DataArray(utcHourValues(1), {
      dims: ['time'],
      coords: { time: zonelessStringCoords(1) }
    });

    const result = await da.sel({ time: new Date(Date.UTC(2021, 0, 1, 8)) });

    expect(result.data).toBe(8);
  });

  test('sel slice matches Date endpoints against zone-less string coordinates', async () => {
    const coords = zonelessStringCoords(1);
    const da = new DataArray(utcHourValues(1), {
      dims: ['time'],
      coords: { time: coords }
    });

    const result = await da.sel({
      time: {
        start: new Date(Date.UTC(2021, 0, 1, 8)),
        stop: new Date(Date.UTC(2021, 0, 1, 10))
      }
    });

    expect(result.data).toEqual([8, 9, 10]);
    expect(result.coords.time).toEqual(coords.slice(8, 11));
  });

  test('sel exact preserves a detached timezone offset', async () => {
    const da = new DataArray(utcHourValues(1), {
      dims: ['time'],
      coords: { time: utcDateCoords(1) }
    });

    const result = await da.sel({ time: '2021-01-01 17:00:00 +05:00' });

    expect(result.data).toBe(12);
  });

  test('sel exact interprets a space-separated zone-less label as UTC', async () => {
    const da = new DataArray(utcHourValues(1), {
      dims: ['time'],
      coords: { time: utcDateCoords(1) }
    });

    const result = await da.sel({ time: '2021-01-01 12:00:00' });

    expect(result.data).toBe(12);
  });

  test('sel exact preserves date-only UTC behavior', async () => {
    const da = new DataArray(utcHourValues(1), {
      dims: ['time'],
      coords: { time: utcDateCoords(1) }
    });

    const result = await da.sel({ time: '2021-01-01' });

    expect(result.data).toBe(0);
  });

  test('sel exact rejects an invalid datetime string as not found', async () => {
    const da = new DataArray(utcHourValues(1), {
      dims: ['time'],
      coords: { time: utcDateCoords(1) }
    });

    await expect(da.sel({ time: 'not-a-datetime' })).rejects.toThrow(
      "Coordinate value 'not-a-datetime' not found in dimension"
    );
  });

  test('sel exact rejects calendar overflow even when a timezone is explicit', async () => {
    const da = new DataArray(utcHourValues(2), {
      dims: ['time'],
      coords: { time: utcDateCoords(2) }
    });

    await expect(da.sel({ time: '2021-02-30T00:00:00+00:00' }))
      .rejects.toThrow('not found in dimension');
  });

  test('sel exact rejects trailing datetime tokens instead of discarding them', async () => {
    const da = new DataArray(utcHourValues(2), {
      dims: ['time'],
      coords: { time: utcDateCoords(2) }
    });

    await expect(da.sel({ time: '2021-01-02 00:00:00 garbage' }))
      .rejects.toThrow('not found in dimension');
  });

  test('sel exact does not coerce categorical datetime lookalikes', async () => {
    const da = new DataArray([1, 2, 3], {
      dims: ['time'],
      coords: {
        time: ['2021-01-01-A', '2021-01-01-B', '2021-01-01-C']
      }
    });

    await expect(
      da.sel({ time: new Date(Date.UTC(2021, 0, 1)) })
    ).rejects.toThrow('not found in dimension');
  });
});
