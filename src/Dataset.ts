/**
 * Dataset - A collection of labeled DataArrays
 * Similar to xarray.Dataset in Python
 */

import { DataArray } from './DataArray';
import {
  Coordinates,
  Attributes,
  DatasetOptions,
  Selection,
  DimensionName,
  CoordinateValue,
  DataValue,
  SelectionOptions
} from './types';
import { deepClone } from './utils';
import { formatCoordinateValue } from './cf-time';
import { ZarrBackend } from './backends/zarr';

export class Dataset {
  private _dataVars: Map<string, DataArray>;
  private _coords: Coordinates;
  private _attrs: Attributes;
  private _coordAttrs: { [coordName: string]: Attributes };

  constructor(
    dataVars: { [name: string]: DataArray } = {},
    options: DatasetOptions = {}
  ) {
    this._dataVars = new Map();
    this._attrs = options.attrs || {};
    this._coords = options.coords || {};
    this._coordAttrs = options.coordAttrs || {};

    // Add data variables
    for (const [name, dataArray] of Object.entries(dataVars)) {
      this.addVariable(name, dataArray);
    }
  }

  /**
   * Get all data variable names
   */
  get dataVars(): string[] {
    return Array.from(this._dataVars.keys());
  }

  /**
   * Get all dimension names
   */
  get dims(): DimensionName[] {
    const dimsSet = new Set<DimensionName>();
    
    for (const dataArray of this._dataVars.values()) {
      for (const dim of dataArray.dims) {
        dimsSet.add(dim);
      }
    }
    
    return Array.from(dimsSet);
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
   * Get dimension sizes
   */
  get sizes(): { [dim: string]: number } {
    const sizes: { [dim: string]: number } = {};
    
    for (const dataArray of this._dataVars.values()) {
      for (let i = 0; i < dataArray.dims.length; i++) {
        const dim = dataArray.dims[i];
        const size = dataArray.shape[i];
        
        if (sizes[dim] !== undefined && sizes[dim] !== size) {
          throw new Error(
            `Inconsistent dimension size for '${dim}': ${sizes[dim]} vs ${size}`
          );
        }
        
        sizes[dim] = size;
      }
    }
    
    return sizes;
  }

  /**
   * Add a data variable
   */
  addVariable(name: string, dataArray: DataArray): void {
    // Validate that dimensions are consistent
    const dataArrayDims = dataArray.dims;
    const dataArrayShape = dataArray.shape;

    for (let i = 0; i < dataArrayDims.length; i++) {
      const dim = dataArrayDims[i];
      const size = dataArrayShape[i];

      // Check against existing variables
      for (const existingArray of this._dataVars.values()) {
        const existingDimIndex = existingArray.dims.indexOf(dim);
        if (existingDimIndex !== -1) {
          const existingSize = existingArray.shape[existingDimIndex];
          if (existingSize !== size) {
            throw new Error(
              `Dimension '${dim}' size mismatch: ${size} vs ${existingSize}`
            );
          }
        }
      }

      // Update coordinates if not already present
      if (!this._coords[dim]) {
        const dataArrayCoords = dataArray.coords;
        if (dataArrayCoords[dim]) {
          this._coords[dim] = dataArrayCoords[dim];
        }
      }
    }

    this._dataVars.set(name, dataArray);
  }

  /**
   * Get a data variable
   */
  getVariable(name: string): DataArray {
    const variable = this._dataVars.get(name);
    if (variable === undefined) {
      throw new Error(`Variable '${name}' not found in dataset`);
    }
    return variable;
  }

  /**
   * Dictionary-style access to data variables (xarray-style)
   * Supports:
   * - ds['varname'] -> returns DataArray
   * - ds[['var1', 'var2']] -> returns new Dataset with subset
   */
  get(key: string | string[]): DataArray | Dataset {
    if (typeof key === 'string') {
      return this.getVariable(key);
    } else if (Array.isArray(key)) {
      const newDataVars: { [name: string]: DataArray } = {};
      for (const varName of key) {
        if (!this.hasVariable(varName)) {
          throw new Error(`Variable '${varName}' not found in dataset`);
        }
        newDataVars[varName] = this._dataVars.get(varName)!;
      }
      return new Dataset(newDataVars, {
        coords: this._coords,
        attrs: this._attrs,
        coordAttrs: this._coordAttrs
      });
    }
    throw new Error('Key must be a string or array of strings');
  }

  /**
   * Check if a variable exists
   */
  hasVariable(name: string): boolean {
    return this._dataVars.has(name);
  }

  /**
   * Remove a data variable
   */
  removeVariable(name: string): boolean {
    return this._dataVars.delete(name);
  }

  /**
   * Select data by coordinate labels
   */
  async sel(selection: Selection, options?: SelectionOptions): Promise<Dataset> {
    const newDataVars: { [name: string]: DataArray } = {};

    for (const [name, dataArray] of this._dataVars.entries()) {
      // Only apply selection to dimensions present in this dataArray
      const relevantSelection: Selection = {};
      for (const dim of dataArray.dims) {
        if (selection[dim] !== undefined) {
          relevantSelection[dim] = selection[dim];
        }
      }

      if (Object.keys(relevantSelection).length > 0) {
        newDataVars[name] = await dataArray.sel(relevantSelection, options);
      } else {
        newDataVars[name] = dataArray;
      }
    }

    // Update coordinates from the selected DataArrays
    const newCoords: Coordinates = {};
    for (const dataArray of Object.values(newDataVars)) {
      for (const dim of dataArray.dims) {
        if (!newCoords[dim]) {
          newCoords[dim] = dataArray.coords[dim];
        }
      }
    }

    return new Dataset(newDataVars, {
      coords: newCoords,
      attrs: this._attrs,
      coordAttrs: this._coordAttrs
    });
  }

  /**
   * Select data by integer position
   */
  async isel(selection: { [dimension: string]: number | number[] }): Promise<Dataset> {
    const newDataVars: { [name: string]: DataArray } = {};

    for (const [name, dataArray] of this._dataVars.entries()) {
      const relevantSelection: { [dimension: string]: number | number[] } = {};
      for (const dim of dataArray.dims) {
        if (selection[dim] !== undefined) {
          relevantSelection[dim] = selection[dim];
        }
      }

      if (Object.keys(relevantSelection).length > 0) {
        newDataVars[name] = await dataArray.isel(relevantSelection);
      } else {
        newDataVars[name] = dataArray;
      }
    }

    return new Dataset(newDataVars, {
      attrs: this._attrs,
      coordAttrs: this._coordAttrs
    });
  }

  /**
   * Apply a function to all data variables
   */
  map(fn: (dataArray: DataArray, name: string) => DataArray): Dataset {
    const newDataVars: { [name: string]: DataArray } = {};

    for (const [name, dataArray] of this._dataVars.entries()) {
      newDataVars[name] = fn(dataArray, name);
    }

    return new Dataset(newDataVars, {
      coords: this._coords,
      attrs: this._attrs,
      coordAttrs: this._coordAttrs
    });
  }

  /**
   * Merge with another dataset
   */
  merge(other: Dataset): Dataset {
    const newDataVars: { [name: string]: DataArray } = {};

    // Add all variables from this dataset
    for (const [name, dataArray] of this._dataVars.entries()) {
      newDataVars[name] = dataArray;
    }

    // Add variables from other dataset
    for (const [name, dataArray] of other._dataVars.entries()) {
      if (this._dataVars.has(name)) {
        throw new Error(`Variable '${name}' already exists in dataset`);
      }
      newDataVars[name] = dataArray;
    }

    // Merge coordinates
    const newCoords: Coordinates = { ...this._coords, ...other._coords };

    // Merge attributes
    const newAttrs: Attributes = { ...this._attrs, ...other._attrs };

    // Merge coordinate attributes
    const newCoordAttrs = { ...this._coordAttrs, ...other._coordAttrs };

    return new Dataset(newDataVars, {
      coords: newCoords,
      attrs: newAttrs,
      coordAttrs: newCoordAttrs
    });
  }

  /**
   * Convert to a plain JavaScript object
   */
  toObject(): any {
    const dataVars: any = {};
    
    for (const [name, dataArray] of this._dataVars.entries()) {
      dataVars[name] = dataArray.toObject();
    }

    return {
      dataVars,
      coords: this.coords,
      attrs: this.attrs,
      dims: this.dims,
      sizes: this.sizes
    };
  }

  /**
   * Convert to JSON string
   */
  toJSON(): string {
    return JSON.stringify(this.toObject());
  }

  /**
   * String representation of the Dataset
   * Works in both Node.js and browser environments
   */
  toString(): string {
    const lines: string[] = [];
    const sizes = this.sizes;

    // Header with dimensions
    lines.push('<jaxray.Dataset>');

    // Dimensions section
    const dimStrs = Object.entries(sizes).map(([dim, size]) => `${dim}: ${size}`);
    lines.push(`Dimensions:  (${dimStrs.join(', ')})`);

    // Coordinates section
    const coordKeys = Object.keys(this._coords);
    if (coordKeys.length > 0) {
      lines.push('Coordinates:');
      for (const key of coordKeys) {
        const coords = this._coords[key];
        
        const dataArray = this._dataVars.get(key);
        const attrs = this._coordAttrs[key];

        const dtype = dataArray ? this._inferDtype(dataArray) : this._inferDtypeFromCoords(coords);
        const coordDims = dataArray ? `(${dataArray.dims.join(', ')})` : `(${key})`;
        const preview = this._formatCoordPreview(coords, attrs);
        lines.push(`  * ${key.padEnd(12)} ${coordDims.padEnd(20)} ${dtype} ${preview}`);
      }
    }

    // Data variables section
    if (this._dataVars.size > 0) {
      lines.push('Data variables:');
      for (const [name, dataArray] of this._dataVars.entries()) {
        const dims = `(${dataArray.dims.join(', ')})`;
        const dtype = this._inferDtype(dataArray);
        lines.push(`    ${name.padEnd(12)} ${dims.padEnd(20)} ${dtype}`);
      }
    }

    // Attributes section
    const attrKeys = Object.keys(this._attrs);
    if (attrKeys.length > 0) {
      lines.push(`Attributes:`);
      for (const key of attrKeys) {
        const value = this._attrs[key];
        const valueStr = typeof value === 'string' ? `'${value}'` : String(value);
        lines.push(`    ${key}:  ${valueStr}`);
      }
    }

    return lines.join('\n');
  }

  /**
   * Infer dtype from DataArray
   */
  private _inferDtype(dataArray: DataArray): string {
    const values = dataArray.values;
    if (!values) return 'object';

    // Flatten the array manually to handle nested arrays
    const flatData: DataValue[] = [];
    const flatten = (arr: any): void => {
      if (Array.isArray(arr)) {
        for (const item of arr) {
          flatten(item);
        }
      } else {
        flatData.push(arr);
      }
    };
    flatten(values);

    if (flatData.length === 0) return 'object';

    const firstValue = flatData.find(v => v != null);
    if (firstValue === undefined) return 'object';

    if (typeof firstValue === 'number') {
      return Number.isInteger(firstValue) ? 'int64' : 'float64';
    } else if (typeof firstValue === 'string') {
      return 'object';
    } else if (typeof firstValue === 'boolean') {
      return 'bool';
    }
    return 'object';
  }

  /**
   * Infer dtype from coordinate values
   */
  private _inferDtypeFromCoords(coords: CoordinateValue[]): string {
    if (coords.length === 0) return 'object';

    const firstValue = coords.find(v => v != null);
    if (firstValue === undefined) return 'object';

    if (typeof firstValue === 'number') {
      return Number.isInteger(firstValue) ? 'int64' : 'float64';
    } else if (typeof firstValue === 'string') {
      return 'object';
    }
    return 'object';
  }

  /**
   * Format coordinate preview
   */
  private _formatCoordPreview(coords: CoordinateValue[], attrs?: Attributes): string {
    if (coords.length === 0) return '[]';

    if (coords.length <= 3) {
      return `[${coords.map(val => formatCoordinateValue(val, attrs)).join(', ')}]`;
    }
    return `[${formatCoordinateValue(coords[0], attrs)}, ${formatCoordinateValue(coords[1], attrs)}, ..., ${formatCoordinateValue(coords[coords.length - 1], attrs)}]`;
  }

  /**
   * Custom Node.js inspector (optional, only works in Node.js)
   */
  [Symbol.for('nodejs.util.inspect.custom')]?(): string {
    return this.toString();
  }

  /**
   * Open a Zarr store as a Dataset
   * Similar to xarray.open_zarr() in Python
   *
   * @param storeOrCid - The Zarr store to open, or a CID string for IPFS-backed sharded zarr
   * @param options - Options for opening the store
   * @returns A Promise that resolves to a Dataset
   *
   * @example
   * ```typescript
   * // Open from a CID using default IPFS gateway
   * const ds = await Dataset.open_zarr('bafyr4ibyb6sk2cxpoab2rvbwvmyjjsup42icy5sj6zyh5jhuqc6ntlkuaa');
   *
   * // Open from a CID using custom IPFS elements
   * const ds = await Dataset.open_zarr('bafyr4i...', { ipfsElements: myCustomElements });
   *
   * // Open from a CID using a custom gateway
   * const ds = await Dataset.open_zarr('bafyr4i...', { ipfsGateway: 'https://ipfs-gateway.dclimate.net' });
   * ```
   */
  static async open_zarr(
    storeOrCid: any,
    options?: {
      group?: string;
      consolidated?: boolean;
      ipfsElements?: any;
      ipfsGateway?: string;
    }
  ): Promise<Dataset> {
    return ZarrBackend.open(storeOrCid, options);
  }
}
