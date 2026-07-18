import { describe, expect, test } from 'vitest';
import { DataArray } from '../../src/DataArray';
import * as cfTime from '../../src/time/cf-time';
import { ZarrBackend } from '../../src/backends/zarr';
import { MemoryZarrStore } from '../helpers/MemoryZarrStore';
import fixture from './fixtures/cftime-reference.json';

type Components = [number, number, number, number, number, number, number];

interface CftimeCase {
  units: string;
  calendar: string;
  value: number;
  isoformat?: string;
  components?: Components;
  error?: string;
  error_msg?: string;
}

type DecodeCFTime = (
  value: number,
  unitsStr: string,
  calendar?: string
) => Date | string | null;

const cases = fixture.cases as CftimeCase[];
const calendarNames = [
  'standard',
  'gregorian',
  'proleptic_gregorian',
  'julian',
  'noleap',
  '365_day',
  'all_leap',
  '366_day',
  '360_day'
];

function requireDecodeCFTime(): DecodeCFTime {
  const decode = (cfTime as any).decodeCFTime;
  expect(typeof decode, 'decodeCFTime not exported from src/time/cf-time.ts').toBe('function');
  return decode as DecodeCFTime;
}

function context(item: CftimeCase): string {
  return `units=${JSON.stringify(item.units)}, calendar=${item.calendar}, value=${item.value}`;
}

function dateComponents(date: Date): Components {
  return [
    date.getUTCFullYear(),
    date.getUTCMonth() + 1,
    date.getUTCDate(),
    date.getUTCHours(),
    date.getUTCMinutes(),
    date.getUTCSeconds(),
    date.getUTCMilliseconds() * 1000
  ];
}

function gregorianDateFor(components: Components): Date | null {
  const [year, month, day, hour, minute, second, microsecond] = components;
  const date = new Date(0);
  date.setUTCFullYear(year, month - 1, day);
  date.setUTCHours(hour, minute, second, microsecond / 1000);
  return dateComponents(date).every((component, index) => component === components[index])
    ? date
    : null;
}

function expectedLegacyResult(item: CftimeCase): Date | null {
  if (item.error) return null;
  return gregorianDateFor(item.components!);
}

function sameLegacyResult(actual: Date | null, expected: Date | null): boolean {
  if (actual === null || expected === null) return actual === expected;
  return actual.getTime() === expected.getTime();
}

describe('CF calendar decoding against cftime ground truth', () => {
  test('decodeCFTime matches every successful fixture and refuses months/years errors', () => {
    const decodeCFTime = requireDecodeCFTime();

    for (const item of cases.filter((entry) => !entry.error)) {
      const decoded = decodeCFTime(item.value, item.units, item.calendar);
      const expectedDate = gregorianDateFor(item.components!);
      const label = context(item);

      if (expectedDate) {
        expect.soft(decoded, `${label}: expected a JS Date`).toBeInstanceOf(Date);
        if (decoded instanceof Date) {
          expect.soft(dateComponents(decoded), label).toEqual(item.components);
        }
      } else {
        expect.soft(decoded, `${label}: expected calendar-faithful string`).toBe(item.isoformat);
      }
    }

    // Deliberate superset: weeks remain supported as exact seven-day arithmetic,
    // even though cftime refuses "weeks since ..." units.
    const refused = cases.filter(
      (item) => item.error && /^(months?|years?)\s+since\s+/i.test(item.units)
    );
    for (const item of refused) {
      expect.soft(
        decodeCFTime(item.value, item.units, item.calendar),
        `${context(item)}: ${item.error_msg}`
      ).toBeNull();
    }
  });

  test('cfTimeToDate has no calendar drift and refuses non-representable or unsupported results', () => {
    const checked = cases.filter(
      (item) => !item.error || /^(months?|years?)\s+since\s+/i.test(item.units)
    );
    const mismatches = checked.filter((item) => {
      const actual = cfTime.cfTimeToDate(item.value, item.units, item.calendar);
      return !sameLegacyResult(actual, expectedLegacyResult(item));
    });
    const counts = Object.fromEntries(
      calendarNames.map((calendar) => {
        const calendarMismatches = mismatches.filter((item) => item.calendar === calendar);
        return [calendar, {
          successfulFixtureCases: calendarMismatches.filter((item) => !item.error).length,
          refusedMonthsOrYears: calendarMismatches.filter((item) => item.error).length,
          total: calendarMismatches.length
        }];
      })
    );
    const examples = Object.fromEntries(
      calendarNames.map((calendar) => {
        const item = mismatches.find((entry) => entry.calendar === calendar);
        if (!item) return [calendar, 'none'];
        const actual = cfTime.cfTimeToDate(item.value, item.units, item.calendar);
        return [
          calendar,
          `${context(item)} observed=${actual?.toISOString() ?? 'null'} expected=${
            expectedLegacyResult(item)?.toISOString() ?? 'null'
          }`
        ];
      })
    );

    expect(
      mismatches.length,
      `calendar drift/refusal mismatches: counts=${JSON.stringify(counts)} examples=${JSON.stringify(examples)}`
    ).toBe(0);
  });

  test('weeks equal seven times days component-wise for every calendar', () => {
    const decodeCFTime = requireDecodeCFTime();

    for (const calendar of calendarNames) {
      for (const value of [-9, 0.5, 9]) {
        const weeks = decodeCFTime(value, 'weeks since 2000-01-01', calendar);
        const days = decodeCFTime(value * 7, 'days since 2000-01-01', calendar);
        const label = `calendar=${calendar}, weeks=${value}, days=${value * 7}`;

        expect.soft(weeks instanceof Date ? dateComponents(weeks) : weeks, label).toEqual(
          days instanceof Date ? dateComponents(days) : days
        );
      }
    }
  });
});

describe('literal CF calendar contracts', () => {
  test('decodeCFTime handles noleap, 360_day, the standard cutover, aliases, and refusal', () => {
    const decodeCFTime = requireDecodeCFTime();

    expect(dateComponents(
      decodeCFTime(60, 'days since 2000-01-01', 'noleap') as Date
    )).toEqual([2000, 3, 2, 0, 0, 0, 0]);
    expect(decodeCFTime(59, 'days since 2000-01-01', '360_day'))
      .toBe('2000-02-30T00:00:00');
    expect(dateComponents(
      decodeCFTime(31, 'days since 1582-10-01', 'standard') as Date
    )).toEqual([1582, 11, 11, 0, 0, 0, 0]);
    expect(dateComponents(
      decodeCFTime(60, 'days since 2000-01-01', 'NOLEAP') as Date
    )).toEqual([2000, 3, 2, 0, 0, 0, 0]);
    expect(decodeCFTime(1, 'months since 2000-01-01', 'noleap')).toBeNull();
    expect(decodeCFTime(1, 'years since 2000-01-01', '360_day')).toBeNull();
  });

  test('cfTimeToDate exposes literal drift at the existing compatibility API', () => {
    expect.soft(
      cfTime.cfTimeToDate(60, 'days since 2000-01-01', 'noleap')?.toISOString()
    ).toBe('2000-03-02T00:00:00.000Z');
    expect.soft(cfTime.cfTimeToDate(59, 'days since 2000-01-01', '360_day')).toBeNull();
    expect.soft(
      cfTime.cfTimeToDate(31, 'days since 1582-10-01', 'standard')?.toISOString()
    ).toBe('1582-11-11T00:00:00.000Z');
    expect.soft(cfTime.cfTimeToDate(1, 'months since 2000-01-01', 'noleap')).toBeNull();
    expect.soft(cfTime.cfTimeToDate(1, 'years since 2000-01-01', '360_day')).toBeNull();
  });

  test('decodeCFTime carries fractional reference seconds that round to one millisecond-second boundary', () => {
    const decoded = requireDecodeCFTime()(
      0,
      'seconds since 2000-01-01T00:00:00.9995'
    );

    expect(decoded).toBeInstanceOf(Date);
    expect((decoded as Date).toISOString()).toBe('2000-01-01T00:00:01.000Z');

    const minuteCarry = requireDecodeCFTime()(
      0,
      'seconds since 2000-01-01T00:00:59.9995'
    );
    expect((minuteCarry as Date).toISOString()).toBe('2000-01-01T00:01:00.000Z');
  });

  test('decodeCFTime uses the CF standard calendar by default', () => {
    const decoded = requireDecodeCFTime()(1, 'days since 1582-10-04');

    expect((decoded as Date).toISOString()).toBe('1582-10-15T00:00:00.000Z');
    expect(cfTime.cfTimeToDate(1, 'days since 1582-10-04')?.toISOString())
      .toBe('1582-10-05T00:00:00.000Z');
  });

  test('CF unit symbols, subsecond units, and hour-only offsets are accepted', () => {
    expect((requireDecodeCFTime()(1, 'ms since 2000-01-01') as Date).toISOString())
      .toBe('2000-01-01T00:00:00.001Z');
    expect((requireDecodeCFTime()(1000, 'us since 2000-01-01') as Date).toISOString())
      .toBe('2000-01-01T00:00:00.001Z');
    expect((requireDecodeCFTime()(0, 'h since 2000-01-01 00:00:00 -06') as Date).toISOString())
      .toBe('2000-01-01T06:00:00.000Z');
    expect(cfTime.parseCFTimeUnits('h since 2000-01-01 00:00:00 -06')?.unit)
      .toBe('hour');
  });

  test('CF unit parsing rejects trailing reference-date tokens', () => {
    expect(cfTime.parseCFTimeUnits('days since 2000-01-01 00:00:00 garbage')).toBeNull();
    expect(requireDecodeCFTime()(0, 'days since 2000-01-01 00:00:00 garbage')).toBeNull();
  });

  test('decodeCFTime pins the standard 1582 cutover and refuses gap references', () => {
    const decodeCFTime = requireDecodeCFTime();

    expect((decodeCFTime(3, 'days since 1582-10-01', 'standard') as Date).toISOString())
      .toBe('1582-10-04T00:00:00.000Z');
    expect((decodeCFTime(4, 'days since 1582-10-01', 'standard') as Date).toISOString())
      .toBe('1582-10-15T00:00:00.000Z');
    expect((decodeCFTime(5, 'days since 1582-10-01', 'standard') as Date).toISOString())
      .toBe('1582-10-16T00:00:00.000Z');
    expect(decodeCFTime(0, 'days since 1582-10-10', 'standard')).toBeNull();
    expect(decodeCFTime(0, 'days since 1582-10-10', 'proleptic_gregorian'))
      .toBeInstanceOf(Date);
  });

  test('formatCoordinateValue preserves calendar-correct display strings', () => {
    expect.soft(cfTime.formatCoordinateValue(60, {
      units: 'days since 2000-01-01',
      calendar: 'noleap'
    })).toBe('2000-03-02T00:00:00');
    expect.soft(cfTime.formatCoordinateValue(59, {
      units: 'days since 2000-01-01',
      calendar: '360_day'
    })).toBe('2000-02-30T00:00:00');
    expect.soft(cfTime.formatCoordinateValue(1, {
      units: 'months since 2000-01-01',
      calendar: 'noleap'
    })).toBe('1');
  });
});

describe('Zarr CF calendar coordinate decoding', () => {
  test('ZarrBackend decodes noleap time coordinates without Gregorian leap-day drift', async () => {
    const values = new Float64Array([58, 59, 60]);
    const store = new MemoryZarrStore({
      'zarr.json': { node_type: 'group', attributes: {} },
      'time/zarr.json': {
        node_type: 'array',
        shape: [3],
        data_type: 'float64',
        dimension_names: ['time'],
        attributes: {
          standard_name: 'time',
          units: 'days since 2000-01-01',
          calendar: 'noleap'
        }
      }
    });
    store.set('time/c/0', new Uint8Array(values.buffer.slice(0)));

    const dataset = await ZarrBackend.open(store);

    expect(dataset.coords.time).toEqual([
      '2000-02-28T00:00:00.000Z',
      '2000-03-01T00:00:00.000Z',
      '2000-03-02T00:00:00.000Z'
    ]);
  });

  test('360_day exact selection preserves cftime labels and nearest fails loudly', async () => {
    const store = createTimeSeriesStore(
      '360_day',
      'days since 2000-01-01',
      [58, 59, 60],
      [580, 590, 600]
    );
    const dataset = await ZarrBackend.open(store);
    const variable = dataset.getVariable('temperature');

    expect(dataset.coords.time).toEqual([
      '2000-02-29T00:00:00.000Z',
      '2000-02-30T00:00:00',
      '2000-03-01T00:00:00.000Z'
    ]);
    expect((await variable.sel({ time: '2000-03-01T00:00:00' })).data).toBe(600);
    expect((await variable.sel({ time: dataset.coords.time[1] })).data).toBe(590);
    await expect(
      variable.sel({ time: '2000-02-29T12:00:00' }, { method: 'nearest' })
    ).rejects.toThrow('Nearest neighbor lookup requires numeric or Date coordinates');
  });

  test('all_leap exact selection does not overflow its 1850 leap day', async () => {
    const store = createTimeSeriesStore(
      'all_leap',
      'days since 1850-01-01',
      [58, 59, 60],
      [580, 590, 600]
    );
    const dataset = await ZarrBackend.open(store);
    const variable = dataset.getVariable('temperature');

    expect(dataset.coords.time).toEqual([
      '1850-02-28T00:00:00.000Z',
      '1850-02-29T00:00:00',
      '1850-03-01T00:00:00.000Z'
    ]);
    expect((await variable.sel({ time: '1850-03-01T00:00:00' })).data).toBe(600);
    expect((await variable.sel({ time: new Date('1850-03-01T00:00:00Z') })).data).toBe(600);
  });

  test('month-based coordinate lookup refuses fixed-duration approximations', async () => {
    const variable = new DataArray([10, 20], {
      dims: ['time'],
      coords: { time: [0, 1] },
      attrs: {
        _coordAttrs: {
          time: {
            standard_name: 'time',
            units: 'months since 2000-01-01',
            calendar: 'noleap'
          }
        }
      }
    });

    await expect(variable.sel({ time: new Date('2000-01-31T00:00:00Z') }))
      .rejects.toThrow('not found');
  });
});

describe('DataArray CF calendar record conversion', () => {
  test('toRecords uses noleap and 360_day calendars for numeric time coordinates', () => {
    const noleap = new DataArray([1, 2], {
      dims: ['time'],
      coords: { time: [59, 60] },
      attrs: {
        _coordAttrs: {
          time: {
            standard_name: 'time',
            units: 'days since 2000-01-01',
            calendar: 'noleap'
          }
        }
      }
    });
    const day360 = new DataArray([1, 2], {
      dims: ['time'],
      coords: { time: [59, 60] },
      attrs: {
        _coordAttrs: {
          time: {
            standard_name: 'time',
            units: 'days since 2000-01-01',
            calendar: '360_day'
          }
        }
      }
    });

    expect(noleap.toRecords().map((record) => record.time)).toEqual([
      '2000-03-01T00:00:00.000Z',
      '2000-03-02T00:00:00.000Z'
    ]);
    expect(day360.toRecords().map((record) => record.time)).toEqual([
      '2000-02-30T00:00:00',
      '2000-03-01T00:00:00.000Z'
    ]);
  });
});

function createTimeSeriesStore(
  calendar: string,
  units: string,
  timeValues: number[],
  dataValues: number[]
): MemoryZarrStore {
  const store = new MemoryZarrStore({
    'zarr.json': { node_type: 'group', attributes: {} },
    'time/zarr.json': {
      node_type: 'array',
      shape: [timeValues.length],
      data_type: 'float64',
      dimension_names: ['time'],
      attributes: { standard_name: 'time', units, calendar }
    },
    'temperature/zarr.json': {
      node_type: 'array',
      shape: [dataValues.length],
      data_type: 'float64',
      dimension_names: ['time'],
      attributes: {}
    }
  });
  const time = new Float64Array(timeValues);
  const data = new Float64Array(dataValues);
  store.set('time/c/0', new Uint8Array(time.buffer.slice(0)));
  store.set('temperature/c/0', new Uint8Array(data.buffer.slice(0)));
  return store;
}
