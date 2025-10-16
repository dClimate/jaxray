/**
 * Dataset - A collection of labeled DataArrays
 * Similar to xarray.Dataset in Python
 */
import { DataArray } from './DataArray.js';
import { Coordinates, Attributes, DatasetOptions, Selection, DimensionName, SelectionOptions, StreamOptions, StreamChunk } from './types.js';
export declare class Dataset {
    private _dataVars;
    private _coords;
    private _attrs;
    private _coordAttrs;
    private _precision;
    private _isEncrypted;
    constructor(dataVars?: {
        [name: string]: DataArray;
    }, options?: DatasetOptions);
    /**
     * Get all data variable names
     */
    get dataVars(): string[];
    /**
     * Get all dimension names
     */
    get dims(): DimensionName[];
    /**
     * Get the coordinates
     */
    get coords(): Coordinates;
    get coordAttrs(): {
        [coordName: string]: Attributes;
    };
    /**
     * Get the attributes
     */
    get attrs(): Attributes;
    /**
     * Check if the dataset contains encrypted data
     */
    get isEncrypted(): boolean;
    /**
     * Get dimension sizes
     */
    get sizes(): {
        [dim: string]: number;
    };
    /**
     * Add a data variable
     */
    addVariable(name: string, dataArray: DataArray): void;
    /**
     * Round a number to the specified precision
     */
    private _roundPrecision;
    /**
     * Get a data variable
     */
    getVariable(name: string): DataArray;
    /**
     * Dictionary-style access to data variables (xarray-style)
     * Supports:
     * - ds['varname'] -> returns DataArray
     * - ds[['var1', 'var2']] -> returns new Dataset with subset
     */
    get(key: string | string[]): DataArray | Dataset;
    /**
     * Check if a variable exists
     */
    hasVariable(name: string): boolean;
    /**
     * Remove a data variable
     */
    removeVariable(name: string): boolean;
    /**
     * Select data by coordinate labels
     */
    sel(selection: Selection, options?: SelectionOptions): Promise<Dataset>;
    /**
     * Stream data selection in chunks (useful for large datasets)
     * @param selection - Selection specification
     * @param options - Streaming options including chunk size and dimension
     * @returns AsyncGenerator yielding Dataset chunks with progress information
     *
     * @example
     * ```typescript
     * const stream = dataset.selStream(
     *   { time: ['2020-01-01', '2020-12-31'], lat: 45, lon: -73 },
     *   { chunkSize: 50 } // 50MB chunks
     * );
     *
     * for await (const chunk of stream) {
     *   console.log(`Progress: ${chunk.progress}%`);
     *   const temp = chunk.data.getVariable('temperature');
     *   await writeToFile(temp);
     * }
     * ```
     */
    selStream(selection: Selection, options?: StreamOptions): AsyncGenerator<StreamChunk<Dataset>>;
    /**
     * Select data by integer position
     */
    isel(selection: {
        [dimension: string]: number | number[];
    }): Promise<Dataset>;
    /**
     * Apply a function to all data variables
     */
    map(fn: (dataArray: DataArray, name: string) => DataArray): Dataset;
    /**
     * Merge with another dataset
     */
    merge(other: Dataset): Dataset;
    /**
     * Detect if any data variables use encryption codecs
     * Checks the codecs in the attributes of each data variable
     * @returns true if encryption is detected, false otherwise
     */
    detectEncryption(): boolean;
    /**
     * Convert to a plain JavaScript object
     */
    toObject(): any;
    /**
     * Convert to JSON string
     */
    toJSON(): string;
    /**
     * Convert a data variable to an array of records with coordinates and values
     * Each record contains coordinate values and the data value for that point
     *
     * @param varName - Name of the variable to convert to records
     * @param options - Optional configuration
     * @param options.precision - Number of decimal places to round coordinate values (default: 6)
     * @returns Array of records with coordinate fields and a value field
     *
     * @example
     * ```typescript
     * // For a variable 'temperature' with dims ['time', 'lat', 'lon']
     * dataset.toRecords('temperature')
     * // Returns:
     * // [
     * //   { time: '2020-01-01', lat: 45.5, lon: -73.5, value: 23.4 },
     * //   { time: '2020-01-01', lat: 45.5, lon: -74.0, value: 24.1 },
     * //   ...
     * // ]
     *
     * // With custom precision
     * dataset.toRecords('temperature', { precision: 2 })
     * // Returns:
     * // [
     * //   { time: '2020-01-01', lat: 45.50, lon: -73.50, value: 23.4 },
     * //   ...
     * // ]
     * ```
     */
    toRecords(varName: string, options?: {
        precision?: number;
    }): Array<Record<string, any>>;
    /**
     * Calculate and store resolution information for all coordinates in coordAttrs
     * For spatial coordinates (lat/lon), stores numeric resolution (e.g., 0.1 degrees)
     * For time coordinates, stores both numeric and human-readable resolution (e.g., "daily")
     *
     * @returns Object mapping dimension names to their resolution info
     *
     * @example
     * ```typescript
     * const resolutions = dataset.calculateCoordinateResolutions();
     * // Returns:
     * // {
     * //   latitude: { resolution: 0.1, unit: 'degrees' },
     * //   longitude: { resolution: 0.1, unit: 'degrees' },
     * //   time: { resolution: 1, type: 'daily', unit: 'days' }
     * // }
     * // Also updates dataset.coordAttrs with resolution info
     * ```
     */
    calculateCoordinateResolutions(): {
        [dim: string]: any;
    };
    /**
     * String representation of the Dataset
     * Works in both Node.js and browser environments
     */
    toString(): string;
    /**
     * Infer dtype from DataArray
     */
    private _inferDtype;
    /**
     * Infer dtype from coordinate values
     */
    private _inferDtypeFromCoords;
    /**
     * Format coordinate preview
     */
    private _formatCoordPreview;
    /**
     * Estimate the size in bytes of a selection for a given variable
     * Useful for checking data size before downloading
     *
     * @param varName - Name of the variable
     * @param selection - Selection object with dimension names and ranges/indices
     * @returns Estimated size in bytes
     */
    getSizeEstimation(varName: string, selection?: {
        [dim: string]: number | {
            start: number;
            stop: number;
        } | [number, number];
    }): number;
    /**
     * Open a Zarr store as a Dataset
     * Similar to xarray.open_zarr() in Python
     *
     * @param store - A ZarrStore implementation (e.g., ShardedStore for IPFS, S3Store, LocalStore)
     * @param options - Options for opening the store
     * @returns A Promise that resolves to a Dataset
     *
     * @example
     * ```typescript
     * // IPFS: Create ShardedStore and open
     * import { ShardedStore, createIpfsElements } from 'jaxray';
     * const ipfsElements = createIpfsElements('https://ipfs-gateway.dclimate.net');
     * const store = await ShardedStore.open('bafyr4i...', ipfsElements);
     * const ds = await Dataset.open_zarr(store);
     *
     * // Future: S3
     * const store = new S3Store(bucket, key);
     * const ds = await Dataset.open_zarr(store);
     *
     * // Future: Local filesystem
     * const store = new LocalStore('/path/to/zarr');
     * const ds = await Dataset.open_zarr(store);
     * ```
     */
    static open_zarr(store: any, options?: {
        group?: string;
        consolidated?: boolean;
    }): Promise<Dataset>;
    /**
     * Build chunk-specific selection based on reference chunk
     */
    private _buildChunkSelection;
}
//# sourceMappingURL=Dataset.d.ts.map