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
  DimensionName
} from './types';
import { deepClone } from './utils';

export class Dataset {
  private _dataVars: Map<string, DataArray>;
  private _coords: Coordinates;
  private _attrs: Attributes;

  constructor(
    dataVars: { [name: string]: DataArray } = {},
    options: DatasetOptions = {}
  ) {
    this._dataVars = new Map();
    this._attrs = options.attrs || {};
    this._coords = options.coords || {};

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
  getVariable(name: string): DataArray | undefined {
    return this._dataVars.get(name);
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
  sel(selection: Selection): Dataset {
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
        newDataVars[name] = dataArray.sel(relevantSelection);
      } else {
        newDataVars[name] = dataArray;
      }
    }

    // Update coordinates based on selection
    const newCoords: Coordinates = {};
    for (const [dim, coords] of Object.entries(this._coords)) {
      if (selection[dim] !== undefined) {
        const sel = selection[dim];
        if (typeof sel === 'number' || typeof sel === 'string') {
          // Single value - dimension is dropped
          continue;
        } else if (Array.isArray(sel)) {
          newCoords[dim] = sel;
        } else if (typeof sel === 'object' && 'start' in sel) {
          const { start, stop } = sel;
          const startIndex = start !== undefined ? coords.indexOf(start) : 0;
          const stopIndex = stop !== undefined ? coords.indexOf(stop) + 1 : coords.length;
          newCoords[dim] = coords.slice(startIndex, stopIndex);
        }
      } else {
        newCoords[dim] = coords;
      }
    }

    return new Dataset(newDataVars, {
      coords: newCoords,
      attrs: this._attrs
    });
  }

  /**
   * Select data by integer position
   */
  isel(selection: { [dimension: string]: number | number[] }): Dataset {
    const newDataVars: { [name: string]: DataArray } = {};

    for (const [name, dataArray] of this._dataVars.entries()) {
      const relevantSelection: { [dimension: string]: number | number[] } = {};
      for (const dim of dataArray.dims) {
        if (selection[dim] !== undefined) {
          relevantSelection[dim] = selection[dim];
        }
      }

      if (Object.keys(relevantSelection).length > 0) {
        newDataVars[name] = dataArray.isel(relevantSelection);
      } else {
        newDataVars[name] = dataArray;
      }
    }

    return new Dataset(newDataVars, {
      attrs: this._attrs
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
      attrs: this._attrs
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

    return new Dataset(newDataVars, {
      coords: newCoords,
      attrs: newAttrs
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
   * Access a variable using bracket notation (for convenience)
   */
  [Symbol.for('nodejs.util.inspect.custom')](): string {
    const vars = Array.from(this._dataVars.keys()).join(', ');
    const dims = this.dims.join(', ');
    return `Dataset { variables: [${vars}], dims: [${dims}] }`;
  }
}
