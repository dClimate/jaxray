/**
 * Utility functions for jaxray
 */
import { NDArray, DataValue } from './types.js';
/**
 * Set of Zarr codec names that indicate encryption
 */
export declare const ZARR_ENCODINGS: Set<string>;
/**
 * Get byte size for a given data type string
 * @param dataType - Zarr data type string (e.g., 'float32', 'int16', 'uint8')
 * @returns Number of bytes per element
 */
export declare function getBytesPerElement(dataType?: string): number;
/**
 * Get the shape of a multi-dimensional array
 */
export declare function getShape(data: NDArray): number[];
/**
 * Flatten a multi-dimensional array
 */
export declare function flatten(data: NDArray): DataValue[];
/**
 * Reshape a flat array into a multi-dimensional array
 */
export declare function reshape(data: DataValue[], shape: number[]): NDArray;
/**
 * Get element at index from multi-dimensional array
 */
export declare function getAtIndex(data: NDArray, indices: number[]): DataValue;
/**
 * Set element at index in multi-dimensional array
 */
export declare function setAtIndex(data: NDArray, indices: number[], value: DataValue): void;
/**
 * Deep clone an object
 */
export declare function deepClone<T>(obj: T): T;
/**
 * Check if two arrays are equal
 */
export declare function arraysEqual(a: any[], b: any[]): boolean;
//# sourceMappingURL=utils.d.ts.map