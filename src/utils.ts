/**
 * Utility functions for jaxray
 */

import { NDArray, DataValue } from './types';

/**
 * Set of Zarr codec names that indicate encryption
 */
export const ZARR_ENCODINGS = new Set(['xchacha20poly1305']);

/**
 * Get byte size for a given data type string
 * @param dataType - Zarr data type string (e.g., 'float32', 'int16', 'uint8')
 * @returns Number of bytes per element
 */
export function getBytesPerElement(dataType?: string): number {
  if (!dataType) return 4; // default to float32

  // Handle various data type formats
  const normalized = dataType.toLowerCase();

  if (normalized.includes('float64') || normalized.includes('f8')) return 8;
  if (normalized.includes('float32') || normalized.includes('f4')) return 4;
  if (normalized.includes('float16') || normalized.includes('f2')) return 2;

  if (normalized.includes('int64') || normalized.includes('i8') || normalized.includes('uint64') || normalized.includes('u8')) return 8;
  if (normalized.includes('int32') || normalized.includes('i4') || normalized.includes('uint32') || normalized.includes('u4')) return 4;
  if (normalized.includes('int16') || normalized.includes('i2') || normalized.includes('uint16') || normalized.includes('u2')) return 2;
  if (normalized.includes('int8') || normalized.includes('i1') || normalized.includes('uint8') || normalized.includes('u1')) return 1;

  // Default to 4 bytes if unknown
  return 4;
}

/**
 * Get the shape of a multi-dimensional array
 */
export function getShape(data: NDArray): number[] {
  if (!Array.isArray(data)) {
    return [];
  }
  
  const shape: number[] = [];
  let current: any = data;
  
  while (Array.isArray(current)) {
    shape.push(current.length);
    current = current[0];
  }
  
  return shape;
}

/**
 * Flatten a multi-dimensional array
 */
export function flatten(data: NDArray): DataValue[] {
  if (!Array.isArray(data)) {
    return [data];
  }
  
  const result: DataValue[] = [];
  
  function recurse(arr: any): void {
    if (!Array.isArray(arr)) {
      result.push(arr);
      return;
    }
    
    for (const item of arr) {
      recurse(item);
    }
  }
  
  recurse(data);
  return result;
}

/**
 * Reshape a flat array into a multi-dimensional array
 */
export function reshape(data: DataValue[], shape: number[]): NDArray {
  if (shape.length === 0) {
    return data[0];
  }
  
  if (shape.length === 1) {
    return data;
  }
  
  const [first, ...rest] = shape;
  const size = rest.reduce((a, b) => a * b, 1);
  const result: DataValue[][] = [];
  
  for (let i = 0; i < first; i++) {
    const slice = data.slice(i * size, (i + 1) * size);
    result.push(reshape(slice, rest) as DataValue[]);
  }
  
  return result;
}

/**
 * Get element at index from multi-dimensional array
 */
export function getAtIndex(data: NDArray, indices: number[]): DataValue {
  let current: any = data;
  
  for (const index of indices) {
    if (!Array.isArray(current)) {
      throw new Error('Index out of bounds');
    }
    current = current[index];
  }
  
  return current;
}

/**
 * Set element at index in multi-dimensional array
 */
export function setAtIndex(data: NDArray, indices: number[], value: DataValue): void {
  let current: any = data;
  for (let i = 0; i < indices.length - 1; i++) {
    if (!Array.isArray(current)) {
      throw new Error('Index out of bounds');
    }
    current = current[indices[i]];
  }
  
  if (Array.isArray(current)) {
    current[indices[indices.length - 1]] = value;
  } else {
    throw new Error('Index out of bounds');
  }
}

/**
 * Deep clone an object
 */
export function deepClone<T>(obj: T): T {
  return JSON.parse(JSON.stringify(obj));
}

/**
 * Check if two arrays are equal
 */
export function arraysEqual(a: any[], b: any[]): boolean {
  if (a.length !== b.length) {
    return false;
  }

  for (let i = 0; i < a.length; i++) {
    if (Array.isArray(a[i]) && Array.isArray(b[i])) {
      if (!arraysEqual(a[i], b[i])) {
        return false;
      }
    } else if (a[i] !== b[i]) {
      return false;
    }
  }

  return true;
}
