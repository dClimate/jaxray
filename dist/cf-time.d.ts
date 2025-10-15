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
export declare function parseCFTimeUnits(unitsStr: string): {
    unit: string;
    referenceDate: Date;
} | null;
/**
 * Convert CF time value to Date
 * @param value - Numeric time value
 * @param unitsStr - CF units string (e.g., "seconds since 1970-01-01")
 * @param calendar - CF calendar type (currently only supports proleptic_gregorian and standard)
 */
export declare function cfTimeToDate(value: number, unitsStr: string, calendar?: string): Date | null;
/**
 * Format date as ISO 8601 string without milliseconds
 * @param date - Date to format
 * @param includeTime - Whether to include time (default: true)
 */
export declare function formatDate(date: Date, includeTime?: boolean): string;
/**
 * Check if coordinate appears to be a time coordinate based on attributes
 */
export declare function isTimeCoordinate(attrs: any): boolean;
/**
 * Format coordinate value for display
 * @param value - Coordinate value (number, string, or Date)
 * @param attrs - Coordinate attributes (for CF time conversion)
 */
export declare function formatCoordinateValue(value: number | string | Date, attrs?: any): string;
//# sourceMappingURL=cf-time.d.ts.map