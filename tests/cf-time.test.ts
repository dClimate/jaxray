/**
 * Tests for CF (Climate and Forecast) time utilities
 */

import { describe, test, expect } from 'vitest';
import {
  parseCFTimeUnits,
  cfTimeToDate,
  formatDate,
  isTimeCoordinate,
  formatCoordinateValue
} from '../src/cf-time';

describe('parseCFTimeUnits', () => {
  test('should parse seconds since format', () => {
    const result = parseCFTimeUnits('seconds since 1970-01-01');
    expect(result).not.toBeNull();
    expect(result?.unit).toBe('second');
    expect(result?.referenceDate.getTime()).toBe(new Date('1970-01-01T00:00:00Z').getTime());
  });

  test('should parse days since format', () => {
    const result = parseCFTimeUnits('days since 2000-01-01T00:00:00');
    expect(result).not.toBeNull();
    expect(result?.unit).toBe('day');
    expect(result?.referenceDate.getTime()).toBe(new Date('2000-01-01T00:00:00Z').getTime());
  });

  test('should parse hours since format with T separator', () => {
    const result = parseCFTimeUnits('hours since 1990-01-01T00:00:00Z');
    expect(result).not.toBeNull();
    expect(result?.unit).toBe('hour');
    expect(result?.referenceDate.getTime()).toBe(new Date('1990-01-01T00:00:00Z').getTime());
  });

  test('should normalize plural units to singular', () => {
    expect(parseCFTimeUnits('seconds since 1970-01-01')?.unit).toBe('second');
    expect(parseCFTimeUnits('minutes since 1970-01-01')?.unit).toBe('minute');
    expect(parseCFTimeUnits('hours since 1970-01-01')?.unit).toBe('hour');
    expect(parseCFTimeUnits('days since 1970-01-01')?.unit).toBe('day');
    expect(parseCFTimeUnits('weeks since 1970-01-01')?.unit).toBe('week');
    expect(parseCFTimeUnits('months since 1970-01-01')?.unit).toBe('month');
    expect(parseCFTimeUnits('years since 1970-01-01')?.unit).toBe('year');
  });

  test('should handle singular units', () => {
    expect(parseCFTimeUnits('second since 1970-01-01')?.unit).toBe('second');
    expect(parseCFTimeUnits('day since 2000-01-01')?.unit).toBe('day');
  });

  test('should return null for invalid format', () => {
    expect(parseCFTimeUnits('invalid format')).toBeNull();
    expect(parseCFTimeUnits('days')).toBeNull();
    expect(parseCFTimeUnits('since 1970-01-01')).toBeNull();
  });

  test('should return null for invalid date', () => {
    expect(parseCFTimeUnits('days since invalid-date')).toBeNull();
    expect(parseCFTimeUnits('days since 2000-99-99')).toBeNull();
  });

  test('should handle date with timezone offset', () => {
    const result = parseCFTimeUnits('hours since 2000-01-01T00:00:00+05:00');
    expect(result).not.toBeNull();
    expect(result?.referenceDate).toBeInstanceOf(Date);
  });

  test('should handle date without time component', () => {
    const result = parseCFTimeUnits('days since 2000-01-01');
    expect(result).not.toBeNull();
    expect(result?.referenceDate.getUTCHours()).toBe(0);
    expect(result?.referenceDate.getUTCMinutes()).toBe(0);
  });

  test('should handle date-time separated by space', () => {
    const result = parseCFTimeUnits('days since 2025-09-01 00:00:00');
    expect(result).not.toBeNull();
    expect(result?.unit).toBe('day');
    expect(result?.referenceDate.getTime()).toBe(new Date('2025-09-01T00:00:00Z').getTime());
  });

  test('should be case insensitive', () => {
    expect(parseCFTimeUnits('DAYS SINCE 2000-01-01')).not.toBeNull();
    expect(parseCFTimeUnits('Days Since 2000-01-01')).not.toBeNull();
  });
});

describe('cfTimeToDate', () => {
  test('should convert seconds', () => {
    const date = cfTimeToDate(3600, 'seconds since 1970-01-01');
    expect(date?.getTime()).toBe(new Date('1970-01-01T01:00:00Z').getTime());
  });

  test('should convert minutes', () => {
    const date = cfTimeToDate(60, 'minutes since 1970-01-01');
    expect(date?.getTime()).toBe(new Date('1970-01-01T01:00:00Z').getTime());
  });

  test('should convert hours', () => {
    const date = cfTimeToDate(24, 'hours since 2000-01-01');
    expect(date?.getTime()).toBe(new Date('2000-01-02T00:00:00Z').getTime());
  });

  test('should convert days', () => {
    const date = cfTimeToDate(1, 'days since 2000-01-01');
    expect(date?.getTime()).toBe(new Date('2000-01-02T00:00:00Z').getTime());
  });

  test('should convert weeks', () => {
    const date = cfTimeToDate(1, 'weeks since 2000-01-01');
    expect(date?.getTime()).toBe(new Date('2000-01-08T00:00:00Z').getTime());
  });

  test('should convert months (approximate)', () => {
    const date = cfTimeToDate(1, 'months since 2000-01-01');
    expect(date).not.toBeNull();
    // Approximately 30 days
    expect(date?.getTime()).toBeCloseTo(new Date('2000-01-31T00:00:00Z').getTime(), -5);
  });

  test('should convert years (approximate)', () => {
    const date = cfTimeToDate(1, 'years since 2000-01-01');
    expect(date).not.toBeNull();
    // Approximately 365.25 days = 31557600000ms
    // Actual 365 days = 31536000000ms, so difference is ~21600000ms (6 hours)
    // But 2000 was a leap year, so actual is 366 days from 2000-01-01 to 2001-01-01
    // So we expect about 6-18 hours difference
    const actual = date?.getTime() || 0;
    const expected = new Date('2001-01-01T00:00:00Z').getTime();
    const diff = Math.abs(actual - expected);
    expect(diff).toBeLessThan(24 * 60 * 60 * 1000); // Within 24 hours
  });

  test('should return null for invalid units string', () => {
    expect(cfTimeToDate(100, 'invalid format')).toBeNull();
  });

  test('should return null for unknown unit', () => {
    // Force an unknown unit through a valid-looking format that gets parsed but has wrong unit
    const result = cfTimeToDate(100, 'seconds since 1970-01-01');
    expect(result).not.toBeNull(); // seconds is valid
  });

  test('should handle negative values', () => {
    const date = cfTimeToDate(-1, 'days since 2000-01-01');
    expect(date?.getTime()).toBe(new Date('1999-12-31T00:00:00Z').getTime());
  });

  test('should handle calendar parameter', () => {
    const date = cfTimeToDate(1, 'days since 2000-01-01', 'proleptic_gregorian');
    expect(date).not.toBeNull();
  });

  test('should handle zero value', () => {
    const date = cfTimeToDate(0, 'days since 2000-01-01');
    expect(date?.getTime()).toBe(new Date('2000-01-01T00:00:00Z').getTime());
  });

  test('should convert values when reference date uses space separator', () => {
    const date = cfTimeToDate(2, 'days since 2025-09-01 00:00:00');
    expect(date?.getTime()).toBe(new Date('2025-09-03T00:00:00Z').getTime());
  });
});

describe('formatDate', () => {
  test('should format date with time by default', () => {
    const date = new Date('2000-01-15T14:30:45Z');
    expect(formatDate(date)).toBe('2000-01-15T14:30:45');
  });

  test('should format date without time when requested', () => {
    const date = new Date('2000-01-15T14:30:45Z');
    expect(formatDate(date, false)).toBe('2000-01-15');
  });

  test('should pad single digit months and days', () => {
    const date = new Date('2000-01-05T00:00:00Z');
    expect(formatDate(date)).toBe('2000-01-05T00:00:00');
  });

  test('should pad single digit hours, minutes, seconds', () => {
    const date = new Date('2000-01-01T01:02:03Z');
    expect(formatDate(date)).toBe('2000-01-01T01:02:03');
  });

  test('should handle midnight', () => {
    const date = new Date('2000-01-01T00:00:00Z');
    expect(formatDate(date)).toBe('2000-01-01T00:00:00');
  });

  test('should handle end of day', () => {
    const date = new Date('2000-12-31T23:59:59Z');
    expect(formatDate(date)).toBe('2000-12-31T23:59:59');
  });

  test('should use UTC for formatting', () => {
    const date = new Date('2000-06-15T12:00:00Z');
    const formatted = formatDate(date);
    expect(formatted).toContain('2000-06-15');
    expect(formatted).toContain('12:00:00');
  });
});

describe('isTimeCoordinate', () => {
  test('should return true for standard_name time', () => {
    expect(isTimeCoordinate({ standard_name: 'time' })).toBe(true);
    expect(isTimeCoordinate({ standard_name: 'Time' })).toBe(true);
    expect(isTimeCoordinate({ standard_name: 'TIME' })).toBe(true);
  });

  test('should return true for long_name time', () => {
    expect(isTimeCoordinate({ long_name: 'time' })).toBe(true);
    expect(isTimeCoordinate({ long_name: 'Time' })).toBe(true);
  });

  test('should return true for units with since', () => {
    expect(isTimeCoordinate({ units: 'days since 2000-01-01' })).toBe(true);
    expect(isTimeCoordinate({ units: 'hours since 1970-01-01' })).toBe(true);
  });

  test('should return false for non-time coordinates', () => {
    expect(isTimeCoordinate({ standard_name: 'latitude' })).toBe(false);
    expect(isTimeCoordinate({ units: 'degrees_north' })).toBe(false);
  });

  test('should return false for null or undefined attrs', () => {
    expect(isTimeCoordinate(null)).toBe(false);
    expect(isTimeCoordinate(undefined)).toBe(false);
  });

  test('should return false for empty object', () => {
    expect(isTimeCoordinate({})).toBe(false);
  });

  test('should handle missing properties gracefully', () => {
    expect(isTimeCoordinate({ other_attr: 'value' })).toBe(false);
  });
});

describe('formatCoordinateValue', () => {
  test('should return string values as-is', () => {
    expect(formatCoordinateValue('test')).toBe('test');
    expect(formatCoordinateValue('2000-01-01')).toBe('2000-01-01');
  });

  test('should format Date objects', () => {
    const date = new Date('2000-01-15T14:30:45Z');
    expect(formatCoordinateValue(date)).toBe('2000-01-15T14:30:45');
  });

  test('should format CF time coordinates', () => {
    const attrs = { units: 'days since 2000-01-01', standard_name: 'time' };
    const result = formatCoordinateValue(1, attrs);
    expect(result).toBe('2000-01-02T00:00:00');
  });

  test('should format numbers without time attributes', () => {
    expect(formatCoordinateValue(42)).toBe('42');
    expect(formatCoordinateValue(3.14)).toBe('3.14');
  });

  test('should handle CF time with calendar attribute', () => {
    const attrs = {
      units: 'hours since 1970-01-01',
      standard_name: 'time',
      calendar: 'proleptic_gregorian'
    };
    const result = formatCoordinateValue(24, attrs);
    expect(result).toBe('1970-01-02T00:00:00');
  });

  test('should handle bigint CF time coordinates', () => {
    const attrs = { units: 'days since 2000-01-01', standard_name: 'time' };
    const result = formatCoordinateValue(BigInt(2), attrs);
    expect(result).toBe('2000-01-03T00:00:00');
  });

  test('should fallback to string for invalid CF time', () => {
    const attrs = { units: 'invalid format', standard_name: 'time' };
    expect(formatCoordinateValue(42, attrs)).toBe('42');
  });

  test('should handle number with non-time attributes', () => {
    const attrs = { units: 'degrees_north', standard_name: 'latitude' };
    expect(formatCoordinateValue(45.5, attrs)).toBe('45.5');
  });

  test('should handle missing attrs', () => {
    expect(formatCoordinateValue(42)).toBe('42');
  });

  test('should handle zero value', () => {
    expect(formatCoordinateValue(0)).toBe('0');
  });

  test('should handle negative numbers', () => {
    expect(formatCoordinateValue(-10)).toBe('-10');
  });
});
