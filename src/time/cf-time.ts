/**
 * CF (Climate and Forecast) Conventions time utilities
 * Handles CF-compliant time coordinate conversions
 */

type CFTimeUnit =
  | 'microsecond'
  | 'millisecond'
  | 'second'
  | 'minute'
  | 'hour'
  | 'day'
  | 'week'
  | 'month'
  | 'year';

const CF_TIME_UNITS_RE = /^(microseconds?|microsecs?|usecs?|us|milliseconds?|millisecs?|msecs?|ms|seconds?|secs?|s|minutes?|mins?|min|hours?|hrs?|hr|h|days?|d|weeks?|wks?|wk|months?|mons?|mon|years?|yrs?|yr)\s+since\s+(.+)$/i;

function normalizeTimeUnit(unit: string): CFTimeUnit | null {
  const normalized = unit.toLowerCase();
  if (/^(microseconds?|microsecs?|usecs?|us)$/.test(normalized)) return 'microsecond';
  if (/^(milliseconds?|millisecs?|msecs?|ms)$/.test(normalized)) return 'millisecond';
  if (/^(seconds?|secs?|s)$/.test(normalized)) return 'second';
  if (/^(minutes?|mins?|min)$/.test(normalized)) return 'minute';
  if (/^(hours?|hrs?|hr|h)$/.test(normalized)) return 'hour';
  if (/^(days?|d)$/.test(normalized)) return 'day';
  if (/^(weeks?|wks?|wk)$/.test(normalized)) return 'week';
  if (/^(months?|mons?|mon)$/.test(normalized)) return 'month';
  if (/^(years?|yrs?|yr)$/.test(normalized)) return 'year';
  return null;
}

function splitTimeUnits(unitsStr: string): { unit: CFTimeUnit; reference: string } | null {
  const match = unitsStr.match(CF_TIME_UNITS_RE);
  if (!match) return null;
  const unit = normalizeTimeUnit(match[1]);
  return unit ? { unit, reference: match[2].trim() } : null;
}

/**
 * Whether a CF time-units string counts in increments finer than a millisecond.
 * A JavaScript Date only carries millisecond precision, so coordinate lookups on
 * such units must go through {@link encodeCFTime} rather than getTime() diffs;
 * otherwise neighbouring sub-millisecond labels collapse onto the same instant.
 */
export function isSubMillisecondTimeUnit(unitsStr: string): boolean {
  return splitTimeUnits(unitsStr)?.unit === 'microsecond';
}

/**
 * Parse CF-compliant time units string
 * Format: "<units> since <reference_date>"
 * Examples:
 *   - "seconds since 1970-01-01"
 *   - "days since 2000-01-01 00:00:00"
 *   - "hours since 1990-01-01T00:00:00Z"
 */
export function parseCFTimeUnits(unitsStr: string): { unit: string; referenceDate: Date } | null {
  const parsedUnits = splitTimeUnits(unitsStr);
  if (!parsedUnits) return null;

  const { unit } = parsedUnits;
  const dateStr = parsedUnits.reference;

  // Parse reference date - handle various formats (always as UTC)
  let referenceDate: Date;
  try {
    let dateStrUTC = dateStr.trim();

    // If date/time are separated by spaces, consume only an optional timezone.
    // Extra tokens are invalid rather than being silently discarded.
    if (!/[tT]/.test(dateStrUTC) && dateStrUTC.includes(' ')) {
      const parts = dateStrUTC.match(
        /^(\S+)\s+(\S+)(?:\s+([zZ]|[+-]\d{1,2}(?::?\d{2})?))?$/
      );
      if (!parts) return null;
      dateStrUTC = `${parts[1]}T${parts[2]}${parts[3] ?? ''}`;
    } else {
      dateStrUTC = dateStrUTC.replace(
        /\s+([zZ]|[+-]\d{1,2}(?::?\d{2})?)$/,
        '$1'
      );
    }

    // ECMAScript Date requires an explicit minute component on numeric offsets.
    if (/[tT]/.test(dateStrUTC)) {
      dateStrUTC = dateStrUTC.replace(/([+-]\d{1,2})$/, '$1:00');
    }

    // Detect existing timezone designator (Z or ±hh[:mm])
    const hasTimezone = /[tT]/.test(dateStrUTC)
      && /([zZ]|[+-]\d{1,2}(?::?\d{2})?)$/.test(dateStrUTC);

    if (!hasTimezone) {
      if (dateStrUTC.includes('T')) {
        dateStrUTC = `${dateStrUTC}Z`;
      } else {
        dateStrUTC = `${dateStrUTC}T00:00:00Z`;
      }
    }

    referenceDate = new Date(dateStrUTC);

    if (Number.isNaN(referenceDate.getTime())) {
      return null;
    }
  /* v8 ignore next 3 */
  } catch {
    return null;
  }

  return { unit, referenceDate };
}

type CFCalendar =
  | 'standard'
  | 'proleptic_gregorian'
  | 'julian'
  | 'noleap'
  | 'all_leap'
  | '360_day';

interface DateTimeComponents {
  year: number;
  month: number;
  day: number;
  hour: number;
  minute: number;
  second: number;
  millisecond: number;
  /** Sub-millisecond remainder in whole microseconds (0-999). Optional; only
   * populated where microsecond-resolution coordinates must stay distinct. */
  microsecond?: number;
}

interface ParsedCalendarUnits {
  unit: CFTimeUnit;
  reference: DateTimeComponents;
  timezoneOffsetMinutes: number;
  fractionCarrySeconds: number;
}

const MILLISECONDS_PER_DAY = 24 * 60 * 60 * 1000;
const MICROSECONDS_PER_DAY = MILLISECONDS_PER_DAY * 1000;
const GREGORIAN_MONTH_LENGTHS = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
const STANDARD_CUTOVER_DAY = 2299161; // 1582-10-15 Gregorian, after 1582-10-04 Julian

function normalizeCalendar(calendar: string): CFCalendar | null {
  switch (calendar.toLowerCase()) {
    case 'standard':
    case 'gregorian':
      return 'standard';
    case 'proleptic_gregorian':
      return 'proleptic_gregorian';
    case 'julian':
      return 'julian';
    case 'noleap':
    case '365_day':
      return 'noleap';
    case 'all_leap':
    case '366_day':
      return 'all_leap';
    case '360_day':
      return '360_day';
    default:
      return null;
  }
}

/** Shared calendar/label date pattern: `YYYY-MM-DD[ (T| )HH[:MM[:SS[.frac]]]][ ][Z|±hh[:mm]]`. */
const CALENDAR_DATE_RE =
  /^([+-]?\d+)-(\d{1,2})-(\d{1,2})(?:[T\s]+(\d{1,2})(?::(\d{1,2}))?(?::(\d{1,2})(?:\.(\d+))?)?)?\s*(Z|[+-]\d{1,2}(?::?\d{2})?)?$/i;

/** Parse a trailing timezone designator to signed minutes; null if malformed. */
function parseTimezoneOffsetMinutes(timezone: string | undefined): number | null {
  if (!timezone || timezone.toUpperCase() === 'Z') return 0;
  const sign = timezone[0] === '-' ? -1 : 1;
  const digits = timezone.slice(1).split(':');
  const compact = timezone.slice(1).replace(':', '');
  const hasColon = digits.length === 2;
  const timezoneHours = Number(hasColon ? digits[0] : compact.length <= 2 ? compact : compact.slice(0, -2));
  const timezoneMinutes = Number(hasColon ? digits[1] : compact.length <= 2 ? 0 : compact.slice(-2));
  if (timezoneHours > 23 || timezoneMinutes > 59) return null;
  return sign * (timezoneHours * 60 + timezoneMinutes);
}

function parseCalendarUnits(unitsStr: string): ParsedCalendarUnits | null {
  const parsedUnits = splitTimeUnits(unitsStr);
  if (!parsedUnits) return null;

  const dateMatch = parsedUnits.reference.match(CALENDAR_DATE_RE);
  if (!dateMatch) return null;

  const fraction = dateMatch[7] ?? '';
  // Preserve the reference fraction at microsecond resolution. Rounding it to
  // whole milliseconds here would shift every decoded coordinate: a reference of
  // ...00.0005 (500 µs) must not become ...00.001.
  const roundedMicroseconds = Math.round(Number(`0.${fraction || '0'}`) * 1_000_000);
  const carriedSeconds = Math.floor(roundedMicroseconds / 1_000_000);
  const microsecondOfSecond = roundedMicroseconds - carriedSeconds * 1_000_000;
  const millisecond = Math.floor(microsecondOfSecond / 1000);
  const timezoneOffsetMinutes = parseTimezoneOffsetMinutes(dateMatch[8]);
  if (timezoneOffsetMinutes === null) return null;

  return {
    unit: parsedUnits.unit,
    reference: {
      year: Number(dateMatch[1]),
      month: Number(dateMatch[2]),
      day: Number(dateMatch[3]),
      hour: Number(dateMatch[4] ?? 0),
      minute: Number(dateMatch[5] ?? 0),
      second: Number(dateMatch[6] ?? 0),
      millisecond,
      microsecond: microsecondOfSecond - millisecond * 1000
    },
    timezoneOffsetMinutes,
    fractionCarrySeconds: carriedSeconds
  };
}

function isGregorianLeapYear(year: number): boolean {
  return year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
}

function isJulianLeapYear(year: number): boolean {
  return year % 4 === 0;
}

function monthLengths(calendar: CFCalendar, year: number): number[] {
  if (calendar === '360_day') return Array(12).fill(30);
  if (calendar === 'all_leap') return [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  if (calendar === 'noleap') return GREGORIAN_MONTH_LENGTHS;

  const leap = calendar === 'julian'
    ? isJulianLeapYear(year)
    : calendar === 'standard' && year < 1582
      ? isJulianLeapYear(year)
      : isGregorianLeapYear(year);
  const lengths = [...GREGORIAN_MONTH_LENGTHS];
  if (leap) lengths[1] = 29;
  return lengths;
}

function isValidDateTime(components: DateTimeComponents, calendar: CFCalendar): boolean {
  const { year, month, day, hour, minute, second, millisecond } = components;
  if (!Number.isInteger(year) || month < 1 || month > 12 || !Number.isInteger(month)) return false;
  if (day < 1 || day > monthLengths(calendar, year)[month - 1] || !Number.isInteger(day)) return false;
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) return false;
  if (millisecond < 0 || millisecond > 999) return false;
  if (calendar === 'standard' && year === 1582 && month === 10 && day >= 5 && day <= 14) {
    return false;
  }
  return true;
}

function gregorianDayNumber(year: number, month: number, day: number): number {
  // Known divergence: standard/julian BCE dates use astronomical year 0;
  // cftime has no year zero. Climate datasets do not practically reach BCE dates.
  const a = Math.floor((14 - month) / 12);
  const y = year + 4800 - a;
  const m = month + 12 * a - 3;
  return day + Math.floor((153 * m + 2) / 5) + 365 * y
    + Math.floor(y / 4) - Math.floor(y / 100) + Math.floor(y / 400) - 32045;
}

function julianDayNumber(year: number, month: number, day: number): number {
  const a = Math.floor((14 - month) / 12);
  const y = year + 4800 - a;
  const m = month + 12 * a - 3;
  return day + Math.floor((153 * m + 2) / 5) + 365 * y + Math.floor(y / 4) - 32083;
}

function gregorianFromDayNumber(dayNumber: number): Pick<DateTimeComponents, 'year' | 'month' | 'day'> {
  const a = dayNumber + 32044;
  const b = Math.floor((4 * a + 3) / 146097);
  const c = a - Math.floor(146097 * b / 4);
  const d = Math.floor((4 * c + 3) / 1461);
  const e = c - Math.floor(1461 * d / 4);
  const m = Math.floor((5 * e + 2) / 153);
  return {
    day: e - Math.floor((153 * m + 2) / 5) + 1,
    month: m + 3 - 12 * Math.floor(m / 10),
    year: 100 * b + d - 4800 + Math.floor(m / 10)
  };
}

function julianFromDayNumber(dayNumber: number): Pick<DateTimeComponents, 'year' | 'month' | 'day'> {
  const c = dayNumber + 32082;
  const d = Math.floor((4 * c + 3) / 1461);
  const e = c - Math.floor(1461 * d / 4);
  const m = Math.floor((5 * e + 2) / 153);
  return {
    day: e - Math.floor((153 * m + 2) / 5) + 1,
    month: m + 3 - 12 * Math.floor(m / 10),
    year: d - 4800 + Math.floor(m / 10)
  };
}

function fixedCalendarDayNumber(components: DateTimeComponents, calendar: CFCalendar): number {
  const yearLength = calendar === '360_day' ? 360 : calendar === 'all_leap' ? 366 : 365;
  const lengths = monthLengths(calendar, components.year);
  let dayNumber = (components.year - 1) * yearLength + components.day - 1;
  for (let month = 1; month < components.month; month++) dayNumber += lengths[month - 1];
  return dayNumber;
}

function fixedCalendarFromDayNumber(
  dayNumber: number,
  calendar: CFCalendar
): Pick<DateTimeComponents, 'year' | 'month' | 'day'> {
  const yearLength = calendar === '360_day' ? 360 : calendar === 'all_leap' ? 366 : 365;
  const year = Math.floor(dayNumber / yearLength) + 1;
  let dayOfYear = dayNumber - (year - 1) * yearLength;
  const lengths = monthLengths(calendar, year);
  let month = 1;
  while (dayOfYear >= lengths[month - 1]) {
    dayOfYear -= lengths[month - 1];
    month++;
  }
  return { year, month, day: dayOfYear + 1 };
}

function toDayNumber(components: DateTimeComponents, calendar: CFCalendar): number {
  if (calendar === 'proleptic_gregorian') {
    return gregorianDayNumber(components.year, components.month, components.day);
  }
  if (calendar === 'julian') {
    return julianDayNumber(components.year, components.month, components.day);
  }
  if (calendar === 'standard') {
    return components.year > 1582
      || (components.year === 1582 && (components.month > 10 || (components.month === 10 && components.day >= 15)))
      ? gregorianDayNumber(components.year, components.month, components.day)
      : julianDayNumber(components.year, components.month, components.day);
  }
  return fixedCalendarDayNumber(components, calendar);
}

function fromDayNumber(
  dayNumber: number,
  calendar: CFCalendar
): Pick<DateTimeComponents, 'year' | 'month' | 'day'> {
  if (calendar === 'proleptic_gregorian') return gregorianFromDayNumber(dayNumber);
  if (calendar === 'julian') return julianFromDayNumber(dayNumber);
  if (calendar === 'standard') {
    return dayNumber >= STANDARD_CUTOVER_DAY
      ? gregorianFromDayNumber(dayNumber)
      : julianFromDayNumber(dayNumber);
  }
  return fixedCalendarFromDayNumber(dayNumber, calendar);
}

function asGregorianDate(components: DateTimeComponents): Date | null {
  const date = new Date(0);
  date.setUTCFullYear(components.year, components.month - 1, components.day);
  date.setUTCHours(components.hour, components.minute, components.second, components.millisecond);
  if (
    date.getUTCFullYear() !== components.year
    || date.getUTCMonth() + 1 !== components.month
    || date.getUTCDate() !== components.day
    || date.getUTCHours() !== components.hour
    || date.getUTCMinutes() !== components.minute
    || date.getUTCSeconds() !== components.second
    || date.getUTCMilliseconds() !== components.millisecond
  ) {
    return null;
  }
  return date;
}

function formatCalendarDate(components: DateTimeComponents): string {
  const year = components.year < 0
    ? `-${String(Math.abs(components.year)).padStart(4, '0')}`
    : String(components.year).padStart(4, '0');
  const month = String(components.month).padStart(2, '0');
  const day = String(components.day).padStart(2, '0');
  const hour = String(components.hour).padStart(2, '0');
  const minute = String(components.minute).padStart(2, '0');
  const second = String(components.second).padStart(2, '0');
  const totalMicroseconds = components.millisecond * 1000 + (components.microsecond ?? 0);
  const fraction = totalMicroseconds
    ? `.${String(totalMicroseconds).padStart(6, '0')}`
    : '';
  return `${year}-${month}-${day}T${hour}:${minute}:${second}${fraction}`;
}

/**
 * Microseconds per elapsed unit. Whole integers so that offset arithmetic can
 * stay exact (no fractional-millisecond rounding). Returns null for units that
 * are not fixed durations in the given calendar (calendar months/years, except
 * 360_day months which are always 30 days).
 */
function unitToMicroseconds(unit: CFTimeUnit, calendar: CFCalendar): number | null {
  switch (unit) {
    case 'microsecond':
      return 1;
    case 'millisecond':
      return 1000;
    case 'second':
      return 1_000_000;
    case 'minute':
      return 60 * 1_000_000;
    case 'hour':
      return 60 * 60 * 1_000_000;
    case 'day':
      return MICROSECONDS_PER_DAY;
    case 'week':
      return 7 * MICROSECONDS_PER_DAY;
    case 'month':
      return calendar === '360_day' ? 30 * MICROSECONDS_PER_DAY : null;
    case 'year':
    default:
      return null;
  }
}

/**
 * Extract UTC calendar components (and any explicit timezone offset) from a
 * decoded time label. Strings are read literally against the calendar rather
 * than through JavaScript's proleptic-Gregorian Date parser, so non-Gregorian
 * labels such as a `noleap` "2001-02-29" survive round-tripping.
 */
function toCalendarComponents(
  value: Date | string
): { components: DateTimeComponents; timezoneOffsetMinutes: number } | null {
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) return null;
    return {
      components: {
        year: value.getUTCFullYear(),
        month: value.getUTCMonth() + 1,
        day: value.getUTCDate(),
        hour: value.getUTCHours(),
        minute: value.getUTCMinutes(),
        second: value.getUTCSeconds(),
        millisecond: value.getUTCMilliseconds(),
        microsecond: 0
      },
      timezoneOffsetMinutes: 0
    };
  }

  const match = value.trim().match(CALENDAR_DATE_RE);
  if (!match) return null;

  const timezoneOffsetMinutes = parseTimezoneOffsetMinutes(match[8]);
  if (timezoneOffsetMinutes === null) return null;

  const fraction = match[7] ?? '';
  const totalMicroseconds = Math.round(Number(`0.${fraction || '0'}`) * 1e6);
  const millisecond = Math.floor(totalMicroseconds / 1000);

  return {
    components: {
      year: Number(match[1]),
      month: Number(match[2]),
      day: Number(match[3]),
      hour: Number(match[4] ?? 0),
      minute: Number(match[5] ?? 0),
      second: Number(match[6] ?? 0),
      millisecond,
      microsecond: totalMicroseconds - millisecond * 1000
    },
    timezoneOffsetMinutes
  };
}

/**
 * Whether a calendar shares proleptic-Gregorian day spacing everywhere, so that
 * JavaScript Date arithmetic reproduces its intervals exactly. Only
 * `proleptic_gregorian` qualifies: the CF-default `standard` calendar switches
 * from Julian to Gregorian at the 1582 cutover (Oct 4 → Oct 15 is one CF day,
 * eleven JS-Date days), and noleap/all_leap/360_day/julian all differ. Every
 * other calendar must be encoded via {@link encodeCFTime} with day-number
 * arithmetic. Absent calendar metadata defaults to `standard` per CF, so it is
 * not treated as proleptic Gregorian.
 */
export function isProlepticGregorianCalendar(calendar?: string): boolean {
  return normalizeCalendar(calendar ?? 'standard') === 'proleptic_gregorian';
}

/**
 * Reference time-of-day offset in whole microseconds (UTC), folding in the
 * parsed timezone offset. The reference itself has at most millisecond
 * precision, so this is always an exact integer.
 */
function referenceMicrosecondsOfDay(parsed: ParsedCalendarUnits): number {
  const milliseconds = parsed.reference.hour * 60 * 60 * 1000
    + parsed.reference.minute * 60 * 1000
    + (parsed.reference.second + parsed.fractionCarrySeconds) * 1000
    + parsed.reference.millisecond
    - parsed.timezoneOffsetMinutes * 60 * 1000;
  return milliseconds * 1000 + (parsed.reference.microsecond ?? 0);
}

/**
 * Decode a numeric CF time coordinate using arithmetic native to its calendar.
 * Calendar dates that JavaScript cannot represent faithfully are returned as strings.
 */
export function decodeCFTime(
  value: number,
  unitsStr: string,
  calendar: string = 'standard'
): Date | string | null {
  if (!Number.isFinite(value)) return null;
  const normalizedCalendar = normalizeCalendar(calendar);
  const parsed = parseCalendarUnits(unitsStr);
  if (!normalizedCalendar || !parsed || !isValidDateTime(parsed.reference, normalizedCalendar)) {
    return null;
  }

  const unitMicroseconds = unitToMicroseconds(parsed.unit, normalizedCalendar);
  if (unitMicroseconds === null) return null;

  const referenceMicroseconds = referenceMicrosecondsOfDay(parsed);

  // Resolve the elapsed offset in whole microseconds, then split into whole
  // milliseconds plus a sub-millisecond remainder. A JavaScript Date only
  // carries millisecond precision, so any coordinate that does not land on a
  // whole millisecond — a sub-ms microsecond value, but also a fractional
  // second/minute/etc. — is rendered as a full-precision string rather than
  // being rounded into a duplicate of its neighbour.
  const elapsedMicroseconds = Math.round(referenceMicroseconds + value * unitMicroseconds);
  const elapsedMilliseconds = Math.floor(elapsedMicroseconds / 1000);
  const microsecondRemainder = elapsedMicroseconds - elapsedMilliseconds * 1000;

  const dayOffset = Math.floor(elapsedMilliseconds / MILLISECONDS_PER_DAY);
  let timeOfDay = elapsedMilliseconds - dayOffset * MILLISECONDS_PER_DAY;
  const dayNumber = toDayNumber(parsed.reference, normalizedCalendar) + dayOffset;
  const date = fromDayNumber(dayNumber, normalizedCalendar);
  const hour = Math.floor(timeOfDay / (60 * 60 * 1000));
  timeOfDay -= hour * 60 * 60 * 1000;
  const minute = Math.floor(timeOfDay / (60 * 1000));
  timeOfDay -= minute * 60 * 1000;
  const second = Math.floor(timeOfDay / 1000);
  const components: DateTimeComponents = {
    ...date,
    hour,
    minute,
    second,
    millisecond: timeOfDay - second * 1000,
    microsecond: microsecondRemainder
  };

  // A non-zero microsecond remainder cannot be represented by a Date; keep the
  // full-precision string so neighbouring coordinates stay distinct.
  if (microsecondRemainder !== 0) {
    return formatCalendarDate(components);
  }
  return asGregorianDate(components) ?? formatCalendarDate(components);
}

/**
 * Encode a decoded time label back to its numeric CF value using arithmetic
 * native to the given calendar — the inverse of {@link decodeCFTime}. Used for
 * coordinate lookups so that nearest/fill selection measures gaps in calendar
 * days rather than proleptic-Gregorian days. Returns null when the label cannot
 * be represented in the calendar or the units are not a fixed duration.
 */
export function encodeCFTime(
  value: Date | string,
  unitsStr: string,
  calendar: string = 'standard'
): number | null {
  const normalizedCalendar = normalizeCalendar(calendar);
  const parsed = parseCalendarUnits(unitsStr);
  if (!normalizedCalendar || !parsed || !isValidDateTime(parsed.reference, normalizedCalendar)) {
    return null;
  }

  const unitMicroseconds = unitToMicroseconds(parsed.unit, normalizedCalendar);
  if (unitMicroseconds === null) return null;

  const parsedValue = toCalendarComponents(value);
  if (!parsedValue || !isValidDateTime(parsedValue.components, normalizedCalendar)) {
    return null;
  }
  const { components, timezoneOffsetMinutes } = parsedValue;

  const referenceMicroseconds = referenceMicrosecondsOfDay(parsed);

  const valueMicroseconds = (components.hour * 60 * 60 * 1000
    + components.minute * 60 * 1000
    + components.second * 1000
    + components.millisecond
    - timezoneOffsetMinutes * 60 * 1000) * 1000
    + (components.microsecond ?? 0);

  const dayOffset = toDayNumber(components, normalizedCalendar)
    - toDayNumber(parsed.reference, normalizedCalendar);
  const elapsedMicroseconds = dayOffset * MICROSECONDS_PER_DAY + valueMicroseconds - referenceMicroseconds;
  return elapsedMicroseconds / unitMicroseconds;
}

/**
 * Convert CF time value to Date
 * @param value - Numeric time value
 * @param unitsStr - CF units string (e.g., "seconds since 1970-01-01")
 * @param calendar - CF calendar type
 */
export function cfTimeToDate(value: number, unitsStr: string, calendar: string = 'proleptic_gregorian'): Date | null {
  const decoded = decodeCFTime(value, unitsStr, calendar);
  return decoded instanceof Date ? decoded : null;
}

/**
 * Format date as ISO 8601 string without milliseconds
 * @param date - Date to format
 * @param includeTime - Whether to include time (default: true)
 */
export function formatDate(date: Date, includeTime: boolean = true): string {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');

  if (!includeTime) {
    return `${year}-${month}-${day}`;
  }

  const hours = String(date.getUTCHours()).padStart(2, '0');
  const minutes = String(date.getUTCMinutes()).padStart(2, '0');
  const seconds = String(date.getUTCSeconds()).padStart(2, '0');

  return `${year}-${month}-${day}T${hours}:${minutes}:${seconds}`;
}

/**
 * Check if coordinate appears to be a time coordinate based on attributes
 */
export function isTimeCoordinate(attrs: any): boolean {
  if (!attrs) return false;

  const standardName = attrs.standard_name?.toLowerCase() || '';
  const longName = attrs.long_name?.toLowerCase() || '';
  const units = attrs.units?.toLowerCase() || '';

  return (
    standardName === 'time' ||
    longName === 'time' ||
    units.includes('since')
  );
}

/**
 * Format coordinate value for display
 * @param value - Coordinate value (number, string, or Date)
 * @param attrs - Coordinate attributes (for CF time conversion)
 */
export function formatCoordinateValue(value: number | string | Date | bigint, attrs?: any): string {
  if (typeof value === 'bigint') {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) {
      return value.toString();
    }
    return formatCoordinateValue(numeric, attrs);
  }

  if (typeof value === 'string') {
    return value;
  }

  if (value instanceof Date) {
    return formatDate(value);
  }

  // Check if this is a CF time coordinate
  if (attrs && typeof value === 'number' && isTimeCoordinate(attrs)) {
    const units = attrs.units;
    const calendar = attrs.calendar || 'standard';

    if (units) {
      const decoded = decodeCFTime(value, units, calendar);
      if (decoded instanceof Date) {
        return formatDate(decoded);
      }
      if (typeof decoded === 'string') {
        return decoded;
      }
    }
  }

  return String(value);
}
