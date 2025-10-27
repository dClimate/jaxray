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
  lazyLoader?: LazyLoader;
  /**
   * Mapping from current indices to original indices in the source dataset.
   * Used to track chained lazy selections - when a lazy array is sliced,
   * this maps each new index to the corresponding original index.
   *
   * Example: If originalIndexMapping = { latitude: [10, 11, 12, 13, 14] },
   * then new index 0 refers to original index 10, index 1 refers to original 11, etc.
   *
   * When the lazy loader is called, indices are mapped through this before being
   * passed to the loader, ensuring the loader always receives original indices.
   */
  originalIndexMapping?: { [dimension: string]: number[] };
}

/**
 * Range specification for lazy loaders
 */
export type LazyIndexRange = { start: number; stop: number } | number;

/**
 * Loader function signature for lazy DataArrays
 */
export type LazyLoader = (ranges: { [dimension: string]: LazyIndexRange }) => Promise<NDArray> | NDArray;

/**
 * Options for creating a Dataset
 */
export interface DatasetOptions {
  coords?: Coordinates;
  attrs?: Attributes;
  coordAttrs?: { [coordName: string]: Attributes };
}

/**
 * Selection method for nearest neighbor lookups
 */
export type SelectionMethod = 'nearest' | 'ffill' | 'bfill' | 'pad' | 'backfill';

/**
 * Selection specification for indexing
 */
export type Selection = {
  [dimension: string]: number | number[] | CoordinateValue | CoordinateValue[] | { start?: CoordinateValue; stop?: CoordinateValue; step?: number };
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
  chunkSize?: number; // Target chunk size in MB (default: 100MB)
  dimension?: DimensionName; // Dimension to chunk along (default: auto-detect)
}

/**
 * Chunk result from streaming operations
 */
export interface StreamChunk<T> {
  data: T;
  progress: number; // Progress percentage (0-100)
  bytesProcessed: number;
  totalBytes: number;
  chunkIndex: number;
  totalChunks: number;
}

export interface RollingOptions {
  center?: boolean;
  minPeriods?: number;
}
