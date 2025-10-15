/**
 * DataArray - A labeled, multi-dimensional array
 * Similar to xarray.DataArray in Python
 */
import { NDArray, DimensionName, Coordinates, Attributes, DataArrayOptions, Selection, SelectionOptions, StreamOptions, StreamChunk } from './types';
export declare class DataArray {
    private _data;
    private _dims;
    private _coords;
    private _attrs;
    private _name?;
    private _shape;
    private _precision;
    constructor(data: NDArray, options?: DataArrayOptions);
    /**
     * Get the data as a native JavaScript array
     */
    get data(): NDArray;
    /**
     * Get the values (alias for data)
     */
    get values(): NDArray;
    /**
     * Get the dimensions
     */
    get dims(): DimensionName[];
    /**
     * Get the shape
     */
    get shape(): number[];
    /**
     * Get the coordinates
     */
    get coords(): Coordinates;
    /**
     * Get the attributes
     */
    get attrs(): Attributes;
    /**
     * Get the name
     */
    get name(): string | undefined;
    /**
     * Get the number of dimensions
     */
    get ndim(): number;
    /**
     * Get the total size (number of elements)
     */
    get size(): number;
    /**
     * Select data by coordinate labels
     */
    sel(selection: Selection, options?: SelectionOptions): Promise<DataArray>;
    /**
     * Stream data selection in chunks (useful for large datasets)
     * @param selection - Selection specification
     * @param options - Streaming options including chunk size and dimension
     * @returns AsyncGenerator yielding chunks with progress information
     *
     * @example
     * ```typescript
     * const stream = dataArray.selStream(
     *   { time: ['2020-01-01', '2020-12-31'] },
     *   { chunkSize: 50 } // 50MB chunks
     * );
     *
     * for await (const chunk of stream) {
     *   console.log(`Progress: ${chunk.progress}%`);
     *   processData(chunk.data);
     * }
     * ```
     */
    selStream(selection: Selection, options?: StreamOptions): AsyncGenerator<StreamChunk<DataArray>>;
    /**
     * Select data by integer position
     */
    isel(selection: {
        [dimension: string]: number | number[];
    }): Promise<DataArray>;
    /**
     * Reduce along a dimension
     */
    sum(dim?: DimensionName): DataArray | number;
    /**
     * Mean along a dimension
     */
    mean(dim?: DimensionName): DataArray | number;
    /**
     * Convert to a plain JavaScript object
     */
    toObject(): any;
    /**
     * Convert to JSON string
     */
    toJSON(): string;
    /**
     * Convert to an array of records with coordinates and values
     * Each record contains coordinate values and the data value
     * Time coordinates are automatically converted to ISO datetime strings
     * Numeric coordinates are rounded to avoid floating-point precision errors
     *
     * @param options - Optional configuration
     * @param options.precision - Number of decimal places to round coordinate values (default: 6)
     *
     * @example
     * ```typescript
     * // For a 2D array with dims ['time', 'lat']
     * dataArray.toRecords()
     * // Returns:
     * // [
     * //   { time: '2020-01-01T00:00:00', lat: 45.5, value: 23.4 },
     * //   { time: '2020-01-02T00:00:00', lat: 46.0, value: 24.1 },
     * //   ...
     * // ]
     *
     * // Custom precision
     * dataArray.toRecords({ precision: 2 })
     * // [
     * //   { time: '2020-01-01T00:00:00', lat: 45.50, lon: -73.25, value: 23.4 },
     * //   ...
     * // ]
     * ```
     */
    toRecords(options?: {
        precision?: number;
    }): Array<Record<string, any>>;
    /**
     * Convert CF time value to Date
     * Helper method for toRecords
     */
    private _convertCFTimeToDate;
    /**
     * Get the bounding box for spatial coordinates
     * @param options - Optional configuration
     * @param options.latDim - Name of the latitude dimension (defaults to 'latitude' or 'lat')
     * @param options.lonDim - Name of the longitude dimension (defaults to 'longitude' or 'lon')
     * @param options.precision - Number of decimal places to round to (default: 6, set to null for no rounding)
     * @returns Bounding box with min/max lat/lon, or undefined if spatial dims not found
     *
     * @example
     * ```typescript
     * const bounds = dataArray.getBounds();
     * // Returns: { latMin: 30, latMax: 50, lonMin: -120, lonMax: -70 }
     *
     * const bounds = dataArray.getBounds({ precision: 2 });
     * // Returns: { latMin: 30.12, latMax: 50.45, lonMin: -120.34, lonMax: -70.89 }
     * ```
     */
    getBounds(options?: {
        latDim?: string;
        lonDim?: string;
        precision?: number | null;
    }): {
        latMin: number;
        latMax: number;
        lonMin: number;
        lonMax: number;
    } | undefined;
    /**
     * Round a number to the specified precision
     */
    private _roundPrecision;
    /**
     * Calculate and determine the time resolution from the time coordinate
     * Automatically detects if data is hourly, daily, weekly, monthly, yearly, etc.
     * Uses CF-time utilities to properly handle time units
     * Stores resolution info in attrs
     *
     * @param timeDim - Name of the time dimension (defaults to 'time')
     * @returns Object with resolution value and type, or undefined if cannot be determined
     *
     * @example
     * ```typescript
     * const result = dataArray.calculateTimeResolution();
     * // Returns: { value: 1, type: 'daily', unit: 'days' }
     * // attrs.time_resolution = 1
     * // attrs.time_resolution_type = 'daily'
     * // attrs.time_resolution_unit = 'days'
     * ```
     */
    calculateTimeResolution(timeDim?: string): {
        value: number;
        type: string;
        unit: string;
    } | undefined;
    private _selLazy;
    private _selectData;
    private _findCoordinateIndex;
    /**
     * Fallback method using linear search (original indexOf-based approach)
     */
    private _findIndexFallback;
    /**
     * Find nearest coordinate index
     */
    private _findNearestIndex;
    /**
     * Find forward fill index (last valid index <= value)
     */
    private _findFfillIndex;
    /**
     * Find backward fill index (first valid index >= value)
     */
    private _findBfillIndex;
    private _selectAtDimension;
    private _selectMultipleAtDimension;
    private _sliceAtDimension;
    private _getCoordinateSlice;
    private _reduce;
    private _elementWiseOp;
    private _divideArray;
    /**
     * Select best dimension to chunk along for streaming
     */
    private _selectChunkDimension;
    /**
     * Estimate size in bytes for a DataArray
     */
    private _estimateSize;
}
//# sourceMappingURL=DataArray.d.ts.map