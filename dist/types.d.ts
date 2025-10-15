/**
 * Type definitions for jaxray - A JavaScript implementation similar to xarray
 */
/**
 * Dimension names are strings
 */
export type DimensionName = string;
/**
 * Coordinate values can be numbers, strings, or dates
 */
export type CoordinateValue = number | string | Date;
/**
 * Data values can be numbers, strings, booleans, or null
 */
export type DataValue = number | string | boolean | null;
/**
 * Multi-dimensional data array
 */
export type NDArray = DataValue | DataValue[] | DataValue[][] | DataValue[][][] | DataValue[][][][];
/**
 * Coordinates mapping dimension names to coordinate values
 */
export interface Coordinates {
    [dimension: string]: CoordinateValue[];
}
/**
 * Attributes for metadata
 */
export interface Attributes {
    [key: string]: any;
}
/**
 * Options for creating a DataArray
 */
export interface DataArrayOptions {
    dims?: DimensionName[];
    coords?: Coordinates;
    attrs?: Attributes;
    name?: string;
    lazy?: boolean;
    virtualShape?: number[];
}
/**
 * Options for creating a Dataset
 */
export interface DatasetOptions {
    coords?: Coordinates;
    attrs?: Attributes;
    coordAttrs?: {
        [coordName: string]: Attributes;
    };
}
/**
 * Selection method for nearest neighbor lookups
 */
export type SelectionMethod = 'nearest' | 'ffill' | 'bfill' | 'pad' | 'backfill';
/**
 * Selection specification for indexing
 */
export type Selection = {
    [dimension: string]: number | number[] | CoordinateValue | CoordinateValue[] | {
        start?: CoordinateValue;
        stop?: CoordinateValue;
        step?: number;
    };
};
/**
 * Options for selection operations
 */
export interface SelectionOptions {
    method?: SelectionMethod;
    tolerance?: number;
}
/**
 * Options for streaming selection operations
 */
export interface StreamOptions extends SelectionOptions {
    chunkSize?: number;
    dimension?: DimensionName;
}
/**
 * Chunk result from streaming operations
 */
export interface StreamChunk<T> {
    data: T;
    progress: number;
    bytesProcessed: number;
    totalBytes: number;
    chunkIndex: number;
    totalChunks: number;
}
//# sourceMappingURL=types.d.ts.map