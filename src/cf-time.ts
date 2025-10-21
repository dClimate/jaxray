/**
 * CF (Climate and Forecast) Conventions time utilities
 * Handles CF-compliant time coordinate conversions
 */

/**
 * Parse CF-compliant time units string
 * Format: "<units> since <reference_date>"
 * Examples:
 *   - "seconds since 1970-01-01"
 *   - "days since 2000-01-01 00:00:00"
 *   - "hours since 1990-01-01T00:00:00Z"
 */
export function parseCFTimeUnits(unitsStr: string): { unit: string; referenceDate: Date } | null {
  const match = unitsStr.match(/^(seconds?|minutes?|hours?|days?|weeks?|months?|years?)\s+since\s+(.+)$/i);
  if (!match) return null;

  const unit = match[1].toLowerCase().replace(/s$/, ''); // normalize to singular
  const dateStr = match[2].trim();

  // Parse reference date - handle various formats (always as UTC)
  let referenceDate: Date;
  try {
    let dateStrUTC = dateStr.trim();

    // If date/time separated by space, normalize to ISO 8601 with 'T'
    if (!dateStrUTC.includes('T') && dateStrUTC.includes(' ')) {
      const parts = dateStrUTC.split(/\s+/);
      if (parts.length >= 2) {
        dateStrUTC = `${parts[0]}T${parts[1]}`;
      }
    }

    // Detect existing timezone designator (Z or ±hh[:mm])
    const hasTimezone = /([zZ]|[+-]\d{2}:?\d{2})$/.test(dateStrUTC);

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

/**
 * Convert CF time value to Date
 * @param value - Numeric time value
 * @param unitsStr - CF units string (e.g., "seconds since 1970-01-01")
 * @param calendar - CF calendar type (currently only supports proleptic_gregorian and standard)
 */
export function cfTimeToDate(value: number, unitsStr: string, calendar: string = 'proleptic_gregorian'): Date | null {
  const parsed = parseCFTimeUnits(unitsStr);
  if (!parsed) return null;

  const { unit, referenceDate } = parsed;
  const refTime = referenceDate.getTime();

  let milliseconds: number;
  switch (unit) {
    case 'second':
      milliseconds = value * 1000;
      break;
    case 'minute':
      milliseconds = value * 60 * 1000;
      break;
    case 'hour':
      milliseconds = value * 60 * 60 * 1000;
      break;
    case 'day':
      milliseconds = value * 24 * 60 * 60 * 1000;
      break;
    case 'week':
      milliseconds = value * 7 * 24 * 60 * 60 * 1000;
      break;
    case 'month':
      // Approximate: 30 days per month
      milliseconds = value * 30 * 24 * 60 * 60 * 1000;
      break;
    case 'year':
      // Approximate: 365.25 days per year
      milliseconds = value * 365.25 * 24 * 60 * 60 * 1000;
      break;
    /* v8 ignore next 2 */
    default:
      return null;
  }

  return new Date(refTime + milliseconds);
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
    const calendar = attrs.calendar || 'proleptic_gregorian';

    if (units) {
      const date = cfTimeToDate(value, units, calendar);
      if (date) {
        return formatDate(date);
      }
    }
  }

  return String(value);
}
