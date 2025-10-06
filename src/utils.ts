/**
 * Utility functions for jaxray
 */

import { NDArray, DataValue } from './types';

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
