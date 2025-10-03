/**
 * DataArray - A labeled, multi-dimensional array
 * Similar to xarray.DataArray in Python
 */

import {
  NDArray,
  DataValue,
  DimensionName,
  Coordinates,
  Attributes,
  DataArrayOptions,
  Selection,
  CoordinateValue
} from './types';
import { getShape, flatten, reshape, deepClone } from './utils';
import { isTimeCoordinate, parseCFTimeUnits } from './cf-time';

export class DataArray {
  private _data: NDArray;
  private _dims: DimensionName[];
  private _coords: Coordinates;
  private _attrs: Attributes;
  private _name?: string;
  private _shape: number[];

  constructor(data: NDArray, options: DataArrayOptions = {}) {
    if (options.lazy) {
      if (!options.virtualShape) throw new Error("lazy DataArray requires virtualShape");
      this._data = [];                           // no allocation
      this._shape = [...options.virtualShape];   // real shape
      this._attrs = options.attrs || {};
      this._name = options.name;
      this._dims = options.dims ? [...options.dims] : this._shape.map((_, i) => `dim_${i}`);
      // coords: do NOT enforce lengths; just store if provided, else generate index arrays by size
      this._coords = {};
      if (options.coords) this._coords = deepClone(options.coords);
      for (let i = 0; i < this._dims.length; i++) {
        const d = this._dims[i];
        if (!this._coords[d]) this._coords[d] = Array.from({ length: this._shape[i] }, (_, j) => j);
      }
      return;
    }

    this._data = deepClone(data);
    this._shape = getShape(data);
    this._attrs = options.attrs || {};
    this._name = options.name;

    // Handle dimensions
    if (options.dims) {
      if (options.dims.length !== this._shape.length) {
        throw new Error(
          `Number of dimensions (${options.dims.length}) does not match data shape (${this._shape.length})`
        );
      }
      this._dims = [...options.dims];
    } else {
      // Auto-generate dimension names
      this._dims = this._shape.map((_, i) => `dim_${i}`);
    }

    // Handle coordinates
    this._coords = {};
    if (options.coords) {
      for (const [dim, coords] of Object.entries(options.coords)) {
        const dimIndex = this._dims.indexOf(dim);
        if (dimIndex === -1) {
          throw new Error(`Coordinate dimension '${dim}' not found in dims`);
        }
        if (coords.length !== this._shape[dimIndex]) {
          throw new Error(
            `Coordinate '${dim}' length (${coords.length}) does not match dimension size (${this._shape[dimIndex]})`
          );
        }
        this._coords[dim] = [...coords];
      }
    }

    // Generate default coordinates for dimensions without coordinates
    for (let i = 0; i < this._dims.length; i++) {
      const dim = this._dims[i];
      if (!this._coords[dim]) {
        this._coords[dim] = Array.from({ length: this._shape[i] }, (_, j) => j);
      }
    }
  }

  /**
   * Get the data as a native JavaScript array
   */
  get data(): NDArray {
    return deepClone(this._data);
  }

  /**
   * Get the values (alias for data)
   */
  get values(): NDArray {
    return this.data;
  }

  /**
   * Get the dimensions
   */
  get dims(): DimensionName[] {
    return [...this._dims];
  }

  /**
   * Get the shape
   */
  get shape(): number[] {
    return [...this._shape];
  }

  /**
   * Get the coordinates
   */
  get coords(): Coordinates {
    return deepClone(this._coords);
  }

  /**
   * Get the attributes
   */
  get attrs(): Attributes {
    return deepClone(this._attrs);
  }

  /**
   * Get the name
   */
  get name(): string | undefined {
    return this._name;
  }

  /**
   * Get the number of dimensions
   */
  get ndim(): number {
    return this._dims.length;
  }

  /**
   * Get the total size (number of elements)
   */
  get size(): number {
    return this._shape.reduce((a, b) => a * b, 1);
  }

  /**
   * Select data by coordinate labels
   */
  async sel(selection: Selection): Promise<DataArray> {
    // Check if this is a lazy-loaded array with a loader function
    if (this._attrs._lazy && this._attrs._lazyLoader) {
      return this._selLazy(selection);
    }

    const newData = this._selectData(selection);
    const newDims: DimensionName[] = [];
    const newCoords: Coordinates = {};
    const newShape = getShape(newData);

    let shapeIndex = 0;
    for (const dim of this._dims) {
      if (selection[dim] !== undefined) {
        const sel = selection[dim];
        if (typeof sel === 'number' || typeof sel === 'string') {
          // Single value selection - dimension is dropped
          continue;
        }
      }
      newDims.push(dim);

      if (shapeIndex < newShape.length) {
        // Generate coordinates for the new dimension
        if (selection[dim] !== undefined) {
          const sel = selection[dim];
          if (Array.isArray(sel)) {
            newCoords[dim] = sel as CoordinateValue[];
          } else if (typeof sel === 'object' && 'start' in sel) {
            const { start, stop } = sel;
            const coordSlice = this._getCoordinateSlice(dim, start, stop);
            newCoords[dim] = coordSlice;
          }
        } else {
          newCoords[dim] = this._coords[dim];
        }
        shapeIndex++;
      }
    }

    return new DataArray(newData, {
      dims: newDims,
      coords: newCoords,
      attrs: this._attrs,
      name: this._name
    });
  }

  /**
   * Select data by integer position
   */
  isel(selection: { [dimension: string]: number | number[] }): DataArray {
    const indexSelection: Selection = {};
    
    for (const [dim, sel] of Object.entries(selection)) {
      if (typeof sel === 'number') {
        // Convert index to coordinate value
        indexSelection[dim] = this._coords[dim][sel];
      } else if (Array.isArray(sel)) {
        const coords = sel.map(i => this._coords[dim][i]);
        indexSelection[dim] = coords;
      }
    }

    return this.sel(indexSelection);
  }

  /**
   * Reduce along a dimension
   */
  sum(dim?: DimensionName): DataArray | number {
    if (!dim) {
      // Sum all values
      const flatData = flatten(this._data);
      return flatData.reduce((a, b) => (a as number) + (b as number), 0) as number;
    }

    const dimIndex = this._dims.indexOf(dim);
    if (dimIndex === -1) {
      throw new Error(`Dimension '${dim}' not found`);
    }

    const result = this._reduce(dimIndex, (acc, val) => acc + (val as number));
    const newDims = this._dims.filter((_, i) => i !== dimIndex);
    
    // If all dimensions are reduced, return a scalar
    if (newDims.length === 0) {
      return result as number;
    }
    
    const newCoords: Coordinates = {};
    
    for (const d of newDims) {
      newCoords[d] = this._coords[d];
    }

    return new DataArray(result, {
      dims: newDims,
      coords: newCoords,
      attrs: this._attrs,
      name: this._name
    });
  }

  /**
   * Mean along a dimension
   */
  mean(dim?: DimensionName): DataArray | number {
    if (!dim) {
      const flatData = flatten(this._data);
      const sum = flatData.reduce((a, b) => (a as number) + (b as number), 0) as number;
      return sum / flatData.length;
    }

    const dimIndex = this._dims.indexOf(dim);
    if (dimIndex === -1) {
      throw new Error(`Dimension '${dim}' not found`);
    }

    const dimSize = this._shape[dimIndex];
    const sumResult = this._reduce(dimIndex, (acc, val) => acc + (val as number));
    const meanResult = this._divideArray(sumResult, dimSize);
    
    const newDims = this._dims.filter((_, i) => i !== dimIndex);
    
    // If all dimensions are reduced, return a scalar
    if (newDims.length === 0) {
      return meanResult as number;
    }
    
    const newCoords: Coordinates = {};
    
    for (const d of newDims) {
      newCoords[d] = this._coords[d];
    }

    return new DataArray(meanResult, {
      dims: newDims,
      coords: newCoords,
      attrs: this._attrs,
      name: this._name
    });
  }

  /**
   * Convert to a plain JavaScript object
   */
  toObject(): any {
    return {
      data: this.data,
      dims: this.dims,
      coords: this.coords,
      attrs: this.attrs,
      name: this.name,
      shape: this.shape
    };
  }

  /**
   * Convert to JSON string
   */
  toJSON(): string {
    return JSON.stringify(this.toObject());
  }

  // Private helper methods

  private async _selLazy(selection: Selection): Promise<DataArray> {
    const loader = this._attrs._lazyLoader;

    // Build index ranges for each dimension
    const indexRanges: { [dim: string]: { start: number; stop: number } | number } = {};
    const newDims: DimensionName[] = [];
    const newCoords: Coordinates = {};

    for (let i = 0; i < this._dims.length; i++) {
      const dim = this._dims[i];
      const sel = selection[dim];

      if (sel === undefined) {
        // No selection on this dimension - select all
        indexRanges[dim] = { start: 0, stop: this._shape[i] };
        newDims.push(dim);
        newCoords[dim] = this._coords[dim];
      } else if (typeof sel === 'number' || typeof sel === 'string' || sel instanceof Date) {
        // Single value - dimension will be dropped
        const index = this._findCoordinateIndex(dim, sel);
        indexRanges[dim] = index;
        // Don't add to newDims or newCoords
      } else if (Array.isArray(sel)) {
        // Multiple values selection - convert to range
        const indices = sel.map(v => this._findCoordinateIndex(dim, v));
        const minIdx = Math.min(...indices);
        const maxIdx = Math.max(...indices);
        indexRanges[dim] = { start: minIdx, stop: maxIdx + 1 };
        newDims.push(dim);
        newCoords[dim] = this._coords[dim].slice(minIdx, maxIdx + 1);
      } else if (typeof sel === 'object' && 'start' in sel) {
        // Slice selection
        const { start, stop } = sel;
        const startIndex = start !== undefined ? this._findCoordinateIndex(dim, start) : 0;
        const stopIndex = stop !== undefined ? this._findCoordinateIndex(dim, stop) + 1 : this._shape[i];
        indexRanges[dim] = { start: startIndex, stop: stopIndex };
        newDims.push(dim);
        newCoords[dim] = this._coords[dim].slice(startIndex, stopIndex);
      }
    }

    // Call the loader function provided by the backend
    console.log('Calling lazy loader with index ranges:', indexRanges);
    try {
      const data = await loader(indexRanges);
      console.log('Lazy loader returned data:', data);

      if (!data) {
        throw new Error('Lazy loader returned undefined/null data');
      }

      return new DataArray(data, {
        dims: newDims,
        coords: newCoords,
        attrs: { ...this._attrs, _lazy: false, _lazyLoader: undefined }, // Mark as no longer lazy
        name: this._name
      });
    } catch (error) {
      console.error('Error in lazy loader:', error);
      throw error;
    }
  }

  private _selectData(selection: Selection): NDArray {
    let result: any = this._data;
    let dimensionsDropped = 0;

    for (let i = 0; i < this._dims.length; i++) {
      const dim = this._dims[i];
      const sel = selection[dim];

      if (sel === undefined) {
        continue;
      }

      // Adjust dimension index for already-dropped dimensions
      const currentDimIndex = i - dimensionsDropped;

      if (typeof sel === 'number' || typeof sel === 'string' || sel instanceof Date) {
        // Single value selection - this will drop a dimension
        const index = this._findCoordinateIndex(dim, sel);
        result = this._selectAtDimension(result, currentDimIndex, index);
        dimensionsDropped++;
      } else if (Array.isArray(sel)) {
        // Multiple values selection
        const indices = sel.map(v => this._findCoordinateIndex(dim, v));
        result = this._selectMultipleAtDimension(result, currentDimIndex, indices);
      } else if (typeof sel === 'object' && 'start' in sel) {
        // Slice selection
        const { start, stop } = sel;
        const startIndex = start !== undefined ? this._findCoordinateIndex(dim, start) : 0;
        const stopIndex = stop !== undefined ? this._findCoordinateIndex(dim, stop) + 1 : this._shape[i];
        result = this._sliceAtDimension(result, currentDimIndex, startIndex, stopIndex);
      }
    }

    return result;
  }

  private _findCoordinateIndex(dim: DimensionName, value: CoordinateValue): number {
    const coords = this._coords[dim];

    // Handle time coordinate conversion for string dates
    if (typeof value === 'string') {
      // Get coordinate-specific attributes
      // Check _coordAttrs first (Dataset level), then fall back to _attrs
      const coordAttrs = (this._attrs as any)?._coordAttrs;
      const dimAttrs = coordAttrs?.[dim] || this._attrs;

      // Check if this looks like a time coordinate and we have a date string
      if (isTimeCoordinate(dimAttrs)) {
        const units = dimAttrs?.units as string | undefined;

        if (units) {
          const parsed = parseCFTimeUnits(units);
          if (parsed) {
            // Parse the input date string
            const inputDate = new Date(value);
            if (isNaN(inputDate.getTime())) {
              throw new Error(`Invalid date string: '${value}'`);
            }

            // Calculate the CF time value from the input date
            const { unit, referenceDate } = parsed;
            const timeDiff = inputDate.getTime() - referenceDate.getTime();

            let targetValue: number;
            switch (unit) {
              case 'second':
                targetValue = timeDiff / 1000;
                break;
              case 'minute':
                targetValue = timeDiff / (60 * 1000);
                break;
              case 'hour':
                targetValue = timeDiff / (60 * 60 * 1000);
                break;
              case 'day':
                targetValue = timeDiff / (24 * 60 * 60 * 1000);
                break;
              case 'week':
                targetValue = timeDiff / (7 * 24 * 60 * 60 * 1000);
                break;
              case 'month':
                targetValue = timeDiff / (30 * 24 * 60 * 60 * 1000);
                break;
              case 'year':
                targetValue = timeDiff / (365.25 * 24 * 60 * 60 * 1000);
                break;
              default:
                targetValue = timeDiff / 1000;
            }

            // Find nearest coordinate value
            let closestIndex = 0;
            let minDiff = Math.abs((coords[0] as number) - targetValue);

            for (let i = 1; i < coords.length; i++) {
              const diff = Math.abs((coords[i] as number) - targetValue);
              if (diff < minDiff) {
                minDiff = diff;
                closestIndex = i;
              }
            }

            return closestIndex;
          }
        }
      }
    }

    // Default exact match
    const index = coords.indexOf(value);
    if (index === -1) {
      throw new Error(`Coordinate value '${value}' not found in dimension '${dim}'`);
    }

    return index;
  }

  private _selectAtDimension(data: any, dimIndex: number, index: number): any {
    if (dimIndex === 0) {
      return data[index];
    }

    if (!Array.isArray(data)) {
      throw new Error('Invalid dimension index');
    }

    return data.map((item: any) => this._selectAtDimension(item, dimIndex - 1, index));
  }

  private _selectMultipleAtDimension(data: any, dimIndex: number, indices: number[]): any {
    if (dimIndex === 0) {
      return indices.map(i => data[i]);
    }

    if (!Array.isArray(data)) {
      throw new Error('Invalid dimension index');
    }

    return data.map((item: any) => this._selectMultipleAtDimension(item, dimIndex - 1, indices));
  }

  private _sliceAtDimension(data: any, dimIndex: number, start: number, stop: number): any {
    if (dimIndex === 0) {
      return data.slice(start, stop);
    }

    if (!Array.isArray(data)) {
      throw new Error('Invalid dimension index');
    }

    return data.map((item: any) => this._sliceAtDimension(item, dimIndex - 1, start, stop));
  }

  private _getCoordinateSlice(dim: DimensionName, start?: CoordinateValue, stop?: CoordinateValue): CoordinateValue[] {
    const coords = this._coords[dim];
    const startIndex = start !== undefined ? coords.indexOf(start) : 0;
    const stopIndex = stop !== undefined ? coords.indexOf(stop) + 1 : coords.length;
    return coords.slice(startIndex, stopIndex);
  }

  private _reduce(dimIndex: number, reducer: (acc: number, val: number) => number): any {
    if (dimIndex === 0) {
      // Reducing the first dimension
      const data = this._data as any[];
      if (!Array.isArray(data) || data.length === 0) {
        return data;
      }

      // Check if elements are arrays (multi-dimensional)
      if (Array.isArray(data[0])) {
        // Element-wise reduction across first dimension
        return data.reduce((acc: any, row: any) => {
          if (!acc) return deepClone(row);
          if (Array.isArray(row)) {
            return this._elementWiseOp(acc, row, reducer);
          }
          return reducer(acc as number, row as number);
        });
      } else {
        // Simple 1D reduction
        return data.reduce((acc: number, val: any) => reducer(acc, val as number), 0);
      }
    } else {
      // Reducing a later dimension - recurse into structure
      const data = this._data as any[];
      return data.map((item: any) => {
        const subArray = new DataArray(item, {
          dims: this._dims.slice(1),
          coords: Object.fromEntries(
            Object.entries(this._coords).filter(([k]) => this._dims.slice(1).includes(k))
          )
        });
        const result = subArray._reduce(dimIndex - 1, reducer);
        return result;
      });
    }
  }

  private _elementWiseOp(a: any, b: any, op: (x: number, y: number) => number): any {
    if (!Array.isArray(a) && !Array.isArray(b)) {
      return op(a as number, b as number);
    }
    if (Array.isArray(a) && Array.isArray(b)) {
      return a.map((val, i) => this._elementWiseOp(val, b[i], op));
    }
    throw new Error('Mismatched array dimensions');
  }

  private _divideArray(data: NDArray, divisor: number): NDArray {
    if (!Array.isArray(data)) {
      return (data as number) / divisor;
    }

    return data.map((item: any) => this._divideArray(item, divisor)) as NDArray;
  }
}
